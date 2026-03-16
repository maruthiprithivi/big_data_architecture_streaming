"""
Velocity Showdown — Kafka Producer (Collector Service)
=======================================================
Connects to three live, free public data streams and produces
events to Kafka topics so students can see the anatomy live.

Topics produced:
  velocity.solana    → ~2.5 events/sec  [HIGH velocity]
  velocity.mastodon  → ~1-3 events/sec  [MEDIUM velocity]
  velocity.wikipedia → ~0.3 events/sec  [LOW velocity]

No API keys required. All sources are publicly accessible.
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
MASTODON_ENABLED  = os.getenv("MASTODON_ENABLED",  "true").lower() == "true"
WIKIPEDIA_ENABLED = os.getenv("WIKIPEDIA_ENABLED", "true").lower() == "true"

SOLANA_RPC       = "https://api.mainnet-beta.solana.com"
MASTODON_REST    = "https://fosstodon.org/api/v1/timelines/public"
WIKI_SSE         = "https://stream.wikimedia.org/v2/stream/recentchange"

# ClickHouse HTTP interface for stats queries
CH_HOST = os.getenv("CLICKHOUSE_HOST",    "clickhouse")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD","velocity_pass")
CH_HTTP = f"http://{CH_HOST}:8123/"

TOPICS = {
    "solana":    "velocity.solana",
    "mastodon":  "velocity.mastodon",
    "wikipedia": "velocity.wikipedia",
}

UA = "VelocityShowdown/1.0 (IS459 Big Data Architecture; Educational)"

# ── State ───────────────────────────────────────────────────────
state = {
    "is_running": False,
    "start_time": None,
    "sources": {
        "solana":    {"total": 0, "ts": deque(maxlen=200), "recent": deque(maxlen=15), "running": False},
        "mastodon":  {"total": 0, "ts": deque(maxlen=200), "recent": deque(maxlen=15), "running": False},
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

# ── App ─────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
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


# ── [VELOCITY MEDIUM] Mastodon REST polling ────────────────────
async def collect_mastodon(session: aiohttp.ClientSession, producer: AIOKafkaProducer):
    """
    [VELOCITY] Mastodon public timeline: ~1-3 posts/sec.
    Uses REST polling with since_id — no API key required.
    Switched from SSE to REST to avoid mastodon.social rate-limits on streaming.
    Kafka topic: velocity.mastodon
    """
    src = state["sources"]["mastodon"]
    seen_ids: set = set()
    last_id: str | None = None

    while src["running"]:
        try:
            params = {"limit": "20"}
            if last_id:
                params["since_id"] = last_id

            async with session.get(
                MASTODON_REST,
                params=params,
                headers={"User-Agent": UA},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    await asyncio.sleep(3)
                    continue

                posts = await resp.json()
                if not isinstance(posts, list):
                    await asyncio.sleep(3)
                    continue

                # Oldest first so offsets are monotonically increasing
                for post in reversed(posts):
                    if not src["running"]:
                        break
                    pid = str(post.get("id", ""))
                    if not pid or pid in seen_ids:
                        continue
                    seen_ids.add(pid)

                    # Track the newest id for next poll
                    if last_id is None or int(pid) > int(last_id):
                        last_id = pid

                    content = re.sub(r"<[^>]+>", "", post.get("content", ""))[:120]
                    account = post.get("account", {}).get("username", "anon")
                    lang    = post.get("language") or ""
                    event = {
                        "source":          "mastodon",
                        "event_type":      "post",
                        "post_id":         pid,
                        "account":         account,
                        "language":        lang,
                        "content_preview": content,
                        "timestamp":       datetime.now(timezone.utc).isoformat(),
                        "summary":         f"@{account}: {content[:80]}",
                    }
                    await producer.send(TOPICS["mastodon"], value=event, key="mastodon")
                    record("mastodon", f"@{account}: {content[:80]}")

                # Prevent seen_ids from growing unbounded
                if len(seen_ids) > 2000:
                    seen_ids = set(list(seen_ids)[-500:])

        except Exception:
            pass
        await asyncio.sleep(2)


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
        if MASTODON_ENABLED:
            state["sources"]["mastodon"]["running"] = True
            tasks.append(collect_mastodon(session, producer))
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
    for s in state["sources"].values(): s["running"] = False
    return {"status": "stopped"}


@app.get("/status")
async def status():
    return {
        "is_running": state["is_running"],
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
                    (name == "mastodon"  and MASTODON_ENABLED)  or
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
    empty = {"solana": 0, "mastodon": 0, "wikipedia": 0}
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
                    "mastodon":  rows.get("mastodon",  0),
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
