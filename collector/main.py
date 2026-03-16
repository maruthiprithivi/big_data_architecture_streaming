"""
Velocity Showdown — Kafka Producer (Collector Service)
=======================================================
Connects to three live, free public data streams and produces
events to Kafka topics so students can see the anatomy live.

Topics produced:
  velocity.solana    → ~2.5 events/sec   [HIGH velocity]
  velocity.bluesky   → ~20-30 events/sec [VERY HIGH velocity]
  velocity.wikipedia → ~0.3 events/sec   [LOW velocity]

No API keys required. All sources are publicly accessible.
Streaming protocols used:
  Solana    → HTTP RPC polling (JSON-RPC over HTTPS)
  Bluesky   → WebSocket firehose (Jetstream)
  Wikipedia → Server-Sent Events (SSE)
"""

import asyncio, json, os, re, time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import aiohttp
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import TopicPartition
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ── Config ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP",   "localhost:9092")
SOLANA_ENABLED    = os.getenv("SOLANA_ENABLED",    "true").lower() == "true"
BLUESKY_ENABLED   = os.getenv("BLUESKY_ENABLED",   "true").lower() == "true"
WIKIPEDIA_ENABLED = os.getenv("WIKIPEDIA_ENABLED", "true").lower() == "true"

SOLANA_RPC         = "https://api.mainnet-beta.solana.com"
BLUESKY_JETSTREAM  = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
WIKI_SSE           = "https://stream.wikimedia.org/v2/stream/recentchange"

# ClickHouse HTTP interface for stats queries
CH_HOST = os.getenv("CLICKHOUSE_HOST",    "clickhouse")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD","velocity_pass")
CH_HTTP = f"http://{CH_HOST}:8123/"
STORAGE_LIMIT_BYTES = int(os.getenv("STORAGE_LIMIT_GB", "5")) * 1024 ** 3

TOPICS = {
    "solana":    "velocity.solana",
    "bluesky":   "velocity.bluesky",
    "wikipedia": "velocity.wikipedia",
}

UA = "VelocityShowdown/1.0 (IS459 Big Data Architecture; Educational)"

# ── State ───────────────────────────────────────────────────────
state = {
    "is_running": False,
    "paused_reason": None,
    "start_time": None,
    "sources": {
        "solana":    {"total": 0, "ts": deque(maxlen=200), "recent": deque(maxlen=15), "running": False},
        "bluesky":   {"total": 0, "ts": deque(maxlen=200), "recent": deque(maxlen=15), "running": False},
        "wikipedia": {"total": 0, "ts": deque(maxlen=200), "recent": deque(maxlen=15), "running": False},
    }
}
_producer: AIOKafkaProducer | None = None
_producer_lock = asyncio.Lock()

# ── Kafka metadata ───────────────────────────────────────────────
_meta_consumer: "AIOKafkaConsumer | None" = None
_meta_consumer_lock = asyncio.Lock()
_kafka_meta_cache: dict = {"data": None, "ts": 0.0}


async def get_meta_consumer() -> AIOKafkaConsumer:
    """Persistent consumer used only for end_offsets() queries."""
    global _meta_consumer
    async with _meta_consumer_lock:
        if _meta_consumer is None:
            c = AIOKafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
            try:
                await asyncio.wait_for(c.start(), timeout=5.0)
            except Exception:
                _meta_consumer = None
                raise
            _meta_consumer = c
    return _meta_consumer

# ── Storage watchdog ────────────────────────────────────────────
async def get_storage_bytes() -> int:
    query = "SELECT sum(bytes_on_disk) AS b FROM system.parts WHERE database='velocity_lab' FORMAT JSON"
    async with aiohttp.ClientSession() as s:
        async with s.get(CH_HTTP, params={"query": query},
                         auth=aiohttp.BasicAuth("default", CH_PASS),
                         timeout=aiohttp.ClientTimeout(total=3)) as r:
            d = await r.json(content_type=None)
            return int((d.get("data") or [{}])[0].get("b", 0))


async def storage_watchdog():
    while True:
        await asyncio.sleep(30)
        if not state["is_running"]:
            continue
        try:
            used = await get_storage_bytes()
            if used >= STORAGE_LIMIT_BYTES:
                state["is_running"] = False
                state["paused_reason"] = "storage_limit"
                for s in state["sources"].values():
                    s["running"] = False
                print(f"[watchdog] 5 GB limit reached — auto-paused.")
        except Exception:
            pass


# ── App ─────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    _wdog = asyncio.create_task(storage_watchdog())
    yield
    _wdog.cancel()
    global _producer, _meta_consumer
    async with _meta_consumer_lock:
        if _meta_consumer is not None:
            await _meta_consumer.stop()
            _meta_consumer = None
    async with _producer_lock:
        if _producer is not None:
            await _producer.stop()
            _producer = None

app = FastAPI(title="Velocity Showdown — Collector", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


async def get_producer():
    global _producer
    async with _producer_lock:
        if _producer is None:
            _producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode() if k else None,
            )
            await _producer.start()
    return _producer


def eps(src: str) -> float:
    now = time.time()
    ts = state["sources"][src]["ts"]
    return round(len([t for t in ts if now - t <= 10]) / 10, 2)


def record(src: str, summary: str):
    s = state["sources"][src]
    s["total"] += 1
    s["ts"].append(time.time())
    s["recent"].appendleft(summary[:120])


# ── [VELOCITY HIGH] Solana ─────────────────────────────────────
async def collect_solana(session: aiohttp.ClientSession, producer: AIOKafkaProducer):
    """
    [VELOCITY] Solana produces ~2.5 slots/sec.
    We poll getSlot every 400ms and produce each new slot as a Kafka event.
    Kafka topic: velocity.solana
    """
    src = state["sources"]["solana"]
    last_slot = None
    hdrs = {"Content-Type": "application/json", "User-Agent": UA}

    while src["running"]:
        try:
            async with session.post(
                SOLANA_RPC,
                json={"jsonrpc": "2.0", "id": 1, "method": "getSlot"},
                headers=hdrs, timeout=aiohttp.ClientTimeout(total=5)
            ) as r:
                data = await r.json()
                current_slot = data.get("result")

            if current_slot and last_slot and current_slot > last_slot:
                for i in range(min(current_slot - last_slot, 5)):
                    slot = last_slot + i + 1
                    event = {
                        "source": "solana",
                        "event_type": "slot",
                        "slot": slot,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "summary": f"Slot #{slot:,}",
                    }
                    # [VELOCITY] Produce to Kafka topic
                    await producer.send(TOPICS["solana"], value=event, key="solana")
                    record("solana", f"Slot #{slot:,}")

            last_slot = current_slot or last_slot
        except Exception:
            pass
        await asyncio.sleep(0.4)


# ── [VELOCITY VERY HIGH] Bluesky Jetstream WebSocket ──────────
async def collect_bluesky(session: aiohttp.ClientSession, producer: AIOKafkaProducer):
    """
    [VELOCITY] Bluesky public post firehose: ~20-30 posts/sec.
    Uses Jetstream WebSocket — no API key required, purpose-built for public consumption.
    Protocol: WebSocket (a third streaming pattern alongside SSE and RPC polling).
    Kafka topic: velocity.bluesky
    """
    src = state["sources"]["bluesky"]

    while src["running"]:
        try:
            async with session.ws_connect(
                BLUESKY_JETSTREAM,
                heartbeat=30,
            ) as ws:
                async for msg in ws:
                    if not src["running"]:
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    try:
                        data = json.loads(msg.data)
                        # Only process new post commits
                        commit = data.get("commit", {})
                        if (data.get("kind") != "commit"
                                or commit.get("operation") != "create"
                                or commit.get("collection") != "app.bsky.feed.post"):
                            continue

                        record_data = commit.get("record", {})
                        text = (record_data.get("text") or record_data.get("bridgyOriginalText") or "")[:120]
                        # Strip HTML tags from bridged posts
                        text = re.sub(r"<[^>]+>", "", text)[:120]
                        langs = record_data.get("langs") or []
                        lang = langs[0] if langs else ""

                        # DID is like did:plc:abc123xyz — use short suffix as handle
                        did = data.get("did", "")
                        handle = did.split(":")[-1][:12] if did else "unknown"

                        event = {
                            "source":      "bluesky",
                            "event_type":  "post",
                            "did":         did,
                            "handle":      handle,
                            "language":    lang,
                            "text":        text,
                            "timestamp":   datetime.now(timezone.utc).isoformat(),
                            "summary":     f"[{handle}] {text[:90]}",
                        }
                        await producer.send(TOPICS["bluesky"], value=event, key="bluesky")
                        record("bluesky", f"[{handle}] {text[:90]}")
                    except Exception:
                        pass
        except Exception:
            await asyncio.sleep(3)


# ── [VELOCITY LOW] Wikipedia SSE ───────────────────────────────
async def collect_wikipedia(session: aiohttp.ClientSession, producer: AIOKafkaProducer):
    """
    [VELOCITY] Wikipedia English edits: ~0.3/sec.
    Uses Wikimedia EventStreams SSE — no API key required.
    User-Agent is required by Wikimedia bot policy (403 without it).
    Kafka topic: velocity.wikipedia
    """
    src = state["sources"]["wikipedia"]

    while src["running"]:
        try:
            async with session.get(
                WIKI_SSE,
                timeout=aiohttp.ClientTimeout(total=None),
                headers={
                    "Accept":     "text/event-stream",
                    "User-Agent": UA,
                },
            ) as resp:
                async for raw_line in resp.content:
                    if not src["running"]:
                        break
                    line = raw_line.decode("utf-8", errors="ignore").rstrip()
                    if not line.startswith("data:"):
                        continue
                    try:
                        ev = json.loads(line[5:].strip())
                        if ev.get("wiki") != "enwiki" or ev.get("type") != "edit":
                            continue
                        title = ev.get("title", "")[:80]
                        user  = ev.get("user", "")[:40]
                        old_l = (ev.get("length") or {}).get("old", 0) or 0
                        new_l = (ev.get("length") or {}).get("new", 0) or 0
                        diff  = new_l - old_l
                        sign  = "+" if diff >= 0 else ""
                        event = {
                            "source":      "wikipedia",
                            "event_type":  "edit",
                            "revision_id": str((ev.get("revision") or {}).get("new", "")),
                            "title":       title,
                            "user":        user,
                            "diff_bytes":  diff,
                            "is_minor":    ev.get("minor", False),
                            "timestamp":   datetime.now(timezone.utc).isoformat(),
                            "summary":     f'"{title}" by {user} ({sign}{diff} bytes)',
                        }
                        await producer.send(TOPICS["wikipedia"], value=event, key="wikipedia")
                        record("wikipedia", event["summary"])
                    except Exception:
                        pass
        except Exception:
            await asyncio.sleep(2)


# ── Background runner ───────────────────────────────────────────
async def run_collectors():
    producer = await get_producer()
    state["start_time"] = datetime.now(timezone.utc).isoformat()
    timeout = aiohttp.ClientTimeout(total=None, connect=10)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        if SOLANA_ENABLED:
            state["sources"]["solana"]["running"] = True
            tasks.append(collect_solana(session, producer))
        if BLUESKY_ENABLED:
            state["sources"]["bluesky"]["running"] = True
            tasks.append(collect_bluesky(session, producer))
        if WIKIPEDIA_ENABLED:
            state["sources"]["wikipedia"]["running"] = True
            tasks.append(collect_wikipedia(session, producer))
        await asyncio.gather(*tasks, return_exceptions=True)


# ── REST API ────────────────────────────────────────────────────
@app.post("/start")
async def start():
    if state["is_running"]: return {"status": "already_running"}
    state["is_running"] = True
    asyncio.create_task(run_collectors())
    return {"status": "started"}


@app.post("/stop")
async def stop():
    state["is_running"] = False
    for s in state["sources"].values():
        s["running"] = False
    return {"status": "stopped"}


@app.post("/start/{source}")
async def start_source(source: str):
    if source not in state["sources"]:
        return {"error": f"unknown source: {source}"}
    if not state["is_running"]:
        return {"error": "collection is not running — use POST /start first"}
    src = state["sources"][source]
    if src["running"]:
        return {"status": "already_running", "source": source}
    src["running"] = True
    producer = await get_producer()
    timeout = aiohttp.ClientTimeout(total=None, connect=10)
    session = aiohttp.ClientSession(timeout=timeout)
    fn = {"solana": collect_solana, "bluesky": collect_bluesky, "wikipedia": collect_wikipedia}[source]
    asyncio.create_task(fn(session, producer))
    return {"status": "started", "source": source}


@app.post("/stop/{source}")
async def stop_source(source: str):
    if source not in state["sources"]:
        return {"error": f"unknown source: {source}"}
    state["sources"][source]["running"] = False
    return {"status": "stopped", "source": source}


@app.get("/clickhouse-storage")
async def clickhouse_storage():
    """Returns ClickHouse disk usage for velocity_lab vs the configured limit."""
    try:
        used = await get_storage_bytes()
    except Exception as e:
        return {"error": str(e)}
    limit = STORAGE_LIMIT_BYTES
    return {
        "bytes_on_disk": used,
        "gb": round(used / 1024 ** 3, 3),
        "limit_gb": round(limit / 1024 ** 3, 1),
        "pct_used": round(used / limit * 100, 1) if limit > 0 else 0,
        "over_limit": used >= limit,
    }


@app.get("/status")
async def status():
    return {
        "is_running": state["is_running"],
        "paused_reason": state.get("paused_reason"),
        "start_time": state["start_time"],
        "kafka_broker": KAFKA_BOOTSTRAP,
        "topics": TOPICS,
        "sources": {
            name: {
                "total": s["total"],
                "eps": eps(name),
                "recent": list(s["recent"]),
                "topic": TOPICS[name],
                "enabled": (
                    (name == "solana"    and SOLANA_ENABLED)    or
                    (name == "bluesky"   and BLUESKY_ENABLED)   or
                    (name == "wikipedia" and WIKIPEDIA_ENABLED)
                )
            }
            for name, s in state["sources"].items()
        }
    }


@app.get("/clickhouse-stats")
async def clickhouse_stats():
    """
    Returns per-source row counts from ClickHouse.
    Uses the ClickHouse HTTP interface directly — no driver needed.
    """
    query = (
        "SELECT source, count() AS cnt "
        "FROM velocity_lab.events "
        "GROUP BY source "
        "FORMAT JSON"
    )
    empty = {"solana": 0, "bluesky": 0, "wikipedia": 0}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                CH_HTTP,
                params={"query": query},
                auth=aiohttp.BasicAuth("default", CH_PASS),
                timeout=aiohttp.ClientTimeout(total=3),
            ) as resp:
                if resp.status != 200:
                    return {"by_source": empty, "total": 0}
                data = await resp.json(content_type=None)
                rows = {r["source"]: int(r["cnt"]) for r in data.get("data", [])}
                by_source = {
                    "solana":    rows.get("solana",    0),
                    "bluesky":   rows.get("bluesky",   0),
                    "wikipedia": rows.get("wikipedia", 0),
                }
                return {"by_source": by_source, "total": sum(by_source.values())}
    except Exception as e:
        return {"by_source": empty, "total": 0, "error": str(e)}


@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/")
async def root():
    return {"service": "velocity-collector", "version": "1.0.0",
            "docs": "/docs", "kafka": KAFKA_BOOTSTRAP, "topics": list(TOPICS.values())}


@app.get("/kafka-meta")
async def kafka_meta():
    """
    Returns live Kafka broker info, per-topic partition offsets, and
    consumer group lag for the 'clickhouse-sink' group.
    Result is cached for 2 seconds to avoid admin overhead.
    """
    global _kafka_meta_cache
    now = time.time()
    if _kafka_meta_cache["data"] and now - _kafka_meta_cache["ts"] < 2.0:
        return _kafka_meta_cache["data"]

    empty_topics = {
        topic: {"partition": 0, "producer_offset": 0,
                "consumer_offset": 0, "lag": 0}
        for topic in TOPICS.values()
    }
    fallback = {
        "broker": {
            "node_id": 1,
            "host": KAFKA_BOOTSTRAP.split(":")[0],
            "port": int(KAFKA_BOOTSTRAP.split(":")[1]),
            "mode": "KRaft",
            "status": "connecting",
        },
        "topics": empty_topics,
        "consumer_group": "clickhouse-sink",
    }

    try:
        admin = AIOKafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=5000,
        )
        await asyncio.wait_for(admin.start(), timeout=5.0)
        try:
            # ── Broker info ──────────────────────────────────────
            cluster = await admin.describe_cluster()
            b = cluster["brokers"][0]

            # ── Consumer group committed offsets ─────────────────
            group_offsets = await admin.list_consumer_group_offsets(
                "clickhouse-sink"
            )

            # ── End (producer) offsets ───────────────────────────
            tps = [TopicPartition(t, 0) for t in TOPICS.values()]
            meta_consumer = await get_meta_consumer()
            end_offsets = await asyncio.wait_for(
                meta_consumer.end_offsets(tps), timeout=5.0
            )

            topics_data = {}
            for topic in TOPICS.values():
                tp = TopicPartition(topic, 0)
                prod = end_offsets.get(tp, 0) or 0
                cons_meta = group_offsets.get(tp)
                cons = (cons_meta.offset if cons_meta and cons_meta.offset >= 0
                        else 0)
                topics_data[topic] = {
                    "partition": 0,
                    "producer_offset": prod,
                    "consumer_offset": cons,
                    "lag": max(0, prod - cons),
                }

            result = {
                "broker": {
                    "node_id": b["node_id"],
                    "host":    b["host"],
                    "port":    b["port"],
                    "mode": "KRaft",
                    "status": "ready",
                },
                "topics": topics_data,
                "consumer_group": "clickhouse-sink",
            }
        finally:
            await admin.close()

    except Exception:
        result = fallback

    _kafka_meta_cache = {"data": result, "ts": now}
    return result
