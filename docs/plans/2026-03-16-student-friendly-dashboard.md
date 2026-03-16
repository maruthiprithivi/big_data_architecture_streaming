# Student-Friendly Dashboard Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a live Kafka Anatomy section to the dashboard and a smart Start button that waits for Kafka readiness, so students immediately see Kafka internals (broker, topics, partitions, offsets, lag) alongside velocity data.

**Architecture:** Extend the collector with a `/kafka-meta` endpoint backed by `AIOKafkaAdminClient` + a persistent meta-consumer for end offsets, with a 2s in-memory cache. The dashboard polls this endpoint every second (parallel to `/status`) and renders a new Kafka Anatomy section. The Start button is disabled until `broker.status === "ready"` is returned.

**Tech Stack:** Python 3.11, aiokafka 0.11.0 (AIOKafkaAdminClient + AIOKafkaConsumer), FastAPI, vanilla JS/CSS in a single HTML file, nginx, Docker Compose.

---

## Task 1: Collector — persistent meta-consumer + `/kafka-meta` endpoint

**Files:**
- Modify: `collector/main.py`

### Step 1: Add imports and cache globals

In `collector/main.py`, immediately after the existing globals block (after line 50, `_producer: AIOKafkaProducer | None = None`), add:

```python
# ── Kafka metadata ───────────────────────────────────────────────
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import TopicPartition

_meta_consumer: "AIOKafkaConsumer | None" = None
_kafka_meta_cache: dict = {"data": None, "ts": 0.0}


async def get_meta_consumer() -> "AIOKafkaConsumer":
    """Persistent consumer used only for end_offsets() queries."""
    global _meta_consumer
    if _meta_consumer is None:
        from aiokafka import AIOKafkaConsumer as _C
        _meta_consumer = _C(bootstrap_servers=KAFKA_BOOTSTRAP)
        await _meta_consumer.start()
    return _meta_consumer
```

### Step 2: Add the `/kafka-meta` endpoint

Append to the REST API section of `collector/main.py` (after the `/health` endpoint):

```python
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
        admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        await admin.start()
        try:
            # ── Broker info ──────────────────────────────────────
            cluster = await admin.describe_cluster()
            b = cluster.brokers[0]

            # ── Consumer group committed offsets ─────────────────
            group_offsets = await admin.list_consumer_group_offsets(
                "clickhouse-sink"
            )

            # ── End (producer) offsets ───────────────────────────
            tps = [TopicPartition(t, 0) for t in TOPICS.values()]
            meta_consumer = await get_meta_consumer()
            end_offsets = await meta_consumer.end_offsets(tps)

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
                    "node_id": b.node_id,
                    "host": b.host,
                    "port": b.port,
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
```

### Step 3: Verify the endpoint manually

With the stack running (`docker compose up --build -d`):

```bash
# Should return status: "connecting" before Kafka is healthy
curl -s http://localhost:8000/kafka-meta | python3 -m json.tool

# After ~30s when Kafka is ready:
# Expected: broker.status == "ready", topics have non-zero producer_offset after Start is clicked
```

### Step 4: Commit

```bash
git add collector/main.py
git commit -m "feat(collector): add /kafka-meta endpoint with broker, partition, offset, and lag"
```

---

## Task 2: Dashboard — CSS for Kafka Anatomy section and tooltips

**Files:**
- Modify: `dashboard/index.html`

### Step 1: Add CSS

Inside the `<style>` block in `dashboard/index.html`, append the following before the closing `</style>` tag:

```css
/* ── Kafka Anatomy section ──────────────────────────────────── */
.anatomy{background:var(--card);border:1px solid var(--bdr);border-radius:10px;padding:16px 18px;margin-bottom:18px}
.anatomy-hdr{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.anatomy-title{font-family:var(--mono);font-size:0.53em;letter-spacing:.12em;text-transform:uppercase;color:var(--mu)}
.anatomy-grid{display:grid;grid-template-columns:160px 1fr 1fr 1fr;gap:10px}
.a-card{background:var(--panel);border:1px solid var(--bdr);border-radius:8px;padding:12px 14px}
.a-label{font-family:var(--mono);font-size:0.5em;letter-spacing:.1em;text-transform:uppercase;margin-bottom:8px}
.a-broker .a-label{color:var(--mu)}
.a-sol .a-label{color:var(--teal)}
.a-mast .a-label{color:var(--amber)}
.a-wiki .a-label{color:var(--sage)}
.a-node{font-family:'DM Serif Display',serif;font-size:1.3em;line-height:1.1;margin-bottom:2px}
.a-mode,.a-port{font-size:0.58em;color:var(--mu);font-family:var(--mono)}
.a-status{display:flex;align-items:center;gap:5px;margin-top:8px;font-size:0.58em;font-family:var(--mono)}
.a-sdot{width:6px;height:6px;border-radius:50%;background:var(--bdr);flex-shrink:0;transition:background .4s}
.a-sdot.ready{background:#4ADA90;box-shadow:0 0 5px #4ADA90}
.a-sdot.connecting{background:var(--amber);animation:pulse 1.5s infinite}
.a-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:5px}
.a-key{font-size:0.56em;color:var(--mu);font-family:var(--mono)}
.a-val{font-size:0.62em;font-family:var(--mono);color:var(--txt)}
.a-lag{font-size:0.62em;font-family:var(--mono);font-weight:600;padding:1px 6px;border-radius:4px}
.lag-ok{background:rgba(74,218,144,.12);color:#4ADA90}
.lag-warn{background:rgba(232,168,124,.12);color:var(--amber)}
.lag-hot{background:rgba(218,74,74,.12);color:#DA4A4A}
.a-group{font-size:0.55em;color:var(--mu);font-family:var(--mono);margin-top:10px}
.a-group code{color:var(--teal);background:rgba(74,191,184,.08);padding:1px 5px;border-radius:3px}

/* ── Tooltips (pure CSS) ────────────────────────────────────── */
.tip{position:relative;cursor:help;color:var(--mu);font-size:0.8em;vertical-align:middle;display:inline-block}
.tip::after{content:attr(data-tip);position:absolute;bottom:130%;left:50%;transform:translateX(-50%);background:#0D1820;color:var(--txt);padding:6px 10px;border-radius:6px;font-size:11px;white-space:normal;max-width:220px;text-align:center;opacity:0;pointer-events:none;transition:opacity .2s;z-index:200;border:1px solid var(--bdr);font-family:'DM Sans',sans-serif;line-height:1.4;width:max-content}
.tip:hover::after{opacity:1}

/* ── Start button wrapper tooltip (for disabled state) ───────── */
.btn-wrap{position:relative;display:inline-block}
.btn-wrap[data-tip]::after{content:attr(data-tip);position:absolute;bottom:130%;left:50%;transform:translateX(-50%);background:#0D1820;color:var(--txt);padding:6px 10px;border-radius:6px;font-size:11px;white-space:normal;max-width:220px;text-align:center;opacity:0;pointer-events:none;transition:opacity .2s;z-index:200;border:1px solid var(--bdr);font-family:'DM Sans',sans-serif;line-height:1.4;width:max-content}
.btn-wrap:hover::after{opacity:1}
```

### Step 2: Verify visually

Reload the dashboard — no layout changes yet (no HTML added), but no CSS errors should appear in browser DevTools console.

### Step 3: Commit

```bash
git add dashboard/index.html
git commit -m "feat(dashboard): add CSS for Kafka Anatomy section and tooltip system"
```

---

## Task 3: Dashboard — Kafka Anatomy HTML

**Files:**
- Modify: `dashboard/index.html`

### Step 1: Wrap the Start button

Find this line in the `<header>` section:

```html
<button id="btn-start" onclick="startCollection()">▶ Start Collection</button>
```

Replace it with:

```html
<span class="btn-wrap" id="start-wrap" data-tip="Kafka broker is still starting up — usually ready in 20–30 seconds">
  <button id="btn-start" onclick="startCollection()" disabled>▶ Start Collection</button>
</span>
```

### Step 2: Add Kafka Anatomy section

Find this line in `<main>`:

```html
  <div class="bottom">
```

Insert the following block **immediately before** it:

```html
  <!-- ── Kafka Anatomy ──────────────────────────────────────── -->
  <div class="anatomy">
    <div class="anatomy-hdr">
      <div class="anatomy-title">Kafka Anatomy</div>
      <div style="font-size:0.55em;color:var(--mu);font-family:var(--mono)">consumer group: <code style="color:var(--teal)">clickhouse-sink</code> <span class="tip" data-tip="A named group of consumers. Kafka tracks each group's read position independently, so multiple apps can read the same topic at different speeds.">ⓘ</span></div>
    </div>
    <div class="anatomy-grid">

      <!-- Broker card -->
      <div class="a-card a-broker">
        <div class="a-label">Broker <span class="tip" data-tip="The Kafka server. It receives messages from producers, stores them durably, and serves them to consumers.">ⓘ</span></div>
        <div class="a-node">Node <span id="broker-id">1</span></div>
        <div class="a-mode">KRaft mode <span class="tip" data-tip="Since Kafka 3.3, the broker manages its own consensus — no separate ZooKeeper process needed.">ⓘ</span></div>
        <div class="a-port" id="broker-port">:9092</div>
        <div class="a-status">
          <span class="a-sdot connecting" id="broker-dot"></span>
          <span id="broker-status-lbl">connecting…</span>
        </div>
      </div>

      <!-- velocity.solana topic card -->
      <div class="a-card a-sol">
        <div class="a-label">velocity.solana <span class="tip" data-tip="A Kafka topic is a named, ordered log of messages. Producers write to it; consumers read from it.">ⓘ</span></div>
        <div class="a-row">
          <span class="a-key">Partition <span class="tip" data-tip="Topics are split into partitions for parallel processing. Each partition is an independent ordered log. This demo uses 1 partition per topic.">ⓘ</span></span>
          <span class="a-val" id="sol-partition">0</span>
        </div>
        <div class="a-row">
          <span class="a-key">Produced <span class="tip" data-tip="Every message written to this partition gets a sequential number called an offset. This is the latest offset — how many messages have been produced so far.">ⓘ</span></span>
          <span class="a-val" id="sol-produced">—</span>
        </div>
        <div class="a-row">
          <span class="a-key">Consumed <span class="tip" data-tip="The offset the clickhouse-sink consumer group has committed — i.e. how many messages it has successfully processed and saved to ClickHouse.">ⓘ</span></span>
          <span class="a-val" id="sol-consumed">—</span>
        </div>
        <div class="a-row">
          <span class="a-key">Lag <span class="tip" data-tip="Lag = Produced − Consumed. A low lag means the consumer is keeping up. A growing lag means it is falling behind.">ⓘ</span></span>
          <span class="a-lag lag-ok" id="sol-lag">—</span>
        </div>
      </div>

      <!-- velocity.mastodon topic card -->
      <div class="a-card a-mast">
        <div class="a-label">velocity.mastodon <span class="tip" data-tip="A Kafka topic is a named, ordered log of messages. Producers write to it; consumers read from it.">ⓘ</span></div>
        <div class="a-row">
          <span class="a-key">Partition <span class="tip" data-tip="Topics are split into partitions for parallel processing. Each partition is an independent ordered log. This demo uses 1 partition per topic.">ⓘ</span></span>
          <span class="a-val" id="mast-partition">0</span>
        </div>
        <div class="a-row">
          <span class="a-key">Produced <span class="tip" data-tip="Every message written to this partition gets a sequential number called an offset. This is the latest offset — how many messages have been produced so far.">ⓘ</span></span>
          <span class="a-val" id="mast-produced">—</span>
        </div>
        <div class="a-row">
          <span class="a-key">Consumed <span class="tip" data-tip="The offset the clickhouse-sink consumer group has committed — i.e. how many messages it has successfully processed and saved to ClickHouse.">ⓘ</span></span>
          <span class="a-val" id="mast-consumed">—</span>
        </div>
        <div class="a-row">
          <span class="a-key">Lag <span class="tip" data-tip="Lag = Produced − Consumed. A low lag means the consumer is keeping up. A growing lag means it is falling behind.">ⓘ</span></span>
          <span class="a-lag lag-ok" id="mast-lag">—</span>
        </div>
      </div>

      <!-- velocity.wikipedia topic card -->
      <div class="a-card a-wiki">
        <div class="a-label">velocity.wikipedia <span class="tip" data-tip="A Kafka topic is a named, ordered log of messages. Producers write to it; consumers read from it.">ⓘ</span></div>
        <div class="a-row">
          <span class="a-key">Partition <span class="tip" data-tip="Topics are split into partitions for parallel processing. Each partition is an independent ordered log. This demo uses 1 partition per topic.">ⓘ</span></span>
          <span class="a-val" id="wiki-partition">0</span>
        </div>
        <div class="a-row">
          <span class="a-key">Produced <span class="tip" data-tip="Every message written to this partition gets a sequential number called an offset. This is the latest offset — how many messages have been produced so far.">ⓘ</span></span>
          <span class="a-val" id="wiki-produced">—</span>
        </div>
        <div class="a-row">
          <span class="a-key">Consumed <span class="tip" data-tip="The offset the clickhouse-sink consumer group has committed — i.e. how many messages it has successfully processed and saved to ClickHouse.">ⓘ</span></span>
          <span class="a-val" id="wiki-consumed">—</span>
        </div>
        <div class="a-row">
          <span class="a-key">Lag <span class="tip" data-tip="Lag = Produced − Consumed. A low lag means the consumer is keeping up. A growing lag means it is falling behind.">ⓘ</span></span>
          <span class="a-lag lag-ok" id="wiki-lag">—</span>
        </div>
      </div>

    </div>
  </div>
  <!-- ── end Kafka Anatomy ──────────────────────────────────── -->
```

### Step 3: Verify structure

Open the dashboard in a browser. The Kafka Anatomy section should appear between the velocity meters and the bottom comparison panels, with 4 cards in a row. All values show `—` and the broker dot shows amber "connecting" state. Hover over any `ⓘ` icon to confirm the tooltip appears.

### Step 4: Commit

```bash
git add dashboard/index.html
git commit -m "feat(dashboard): add Kafka Anatomy HTML with broker card and topic cards"
```

---

## Task 4: Dashboard — JavaScript for polling `/kafka-meta` and rendering

**Files:**
- Modify: `dashboard/index.html`

### Step 1: Add helper functions and `updateAnatomy()`

Inside the `<script>` block, find the existing `function feed(id, items)` function. **After** it, add:

```javascript
function fmtOffset(n) {
  if (n === 0 || n === undefined) return '—';
  return n >= 1e6 ? (n/1e6).toFixed(2)+'M' : n >= 1e3 ? (n/1e3).toFixed(1)+'K' : String(n);
}

function lagClass(n) {
  if (n <= 5)  return 'lag-ok';
  if (n <= 20) return 'lag-warn';
  return 'lag-hot';
}

function updateAnatomy(meta) {
  if (!meta) return;
  const b = meta.broker || {};

  // Broker card
  $t('broker-id',      b.node_id ?? 1);
  $t('broker-port',    ':' + (b.port ?? 9092));
  const dot = document.getElementById('broker-dot');
  const lbl = document.getElementById('broker-status-lbl');
  if (b.status === 'ready') {
    dot.className = 'a-sdot ready';
    lbl.textContent = 'connected';
  } else {
    dot.className = 'a-sdot connecting';
    lbl.textContent = 'connecting…';
  }

  // Topic cards
  const map = {
    'velocity.solana':    { pre: 'sol'  },
    'velocity.mastodon':  { pre: 'mast' },
    'velocity.wikipedia': { pre: 'wiki' },
  };
  const topics = meta.topics || {};
  for (const [topic, {pre}] of Object.entries(map)) {
    const t = topics[topic] || {};
    $t(pre+'-partition', t.partition ?? 0);
    $t(pre+'-produced',  fmtOffset(t.producer_offset));
    $t(pre+'-consumed',  fmtOffset(t.consumer_offset));
    const lagEl = document.getElementById(pre+'-lag');
    if (lagEl) {
      const lag = t.lag ?? 0;
      lagEl.textContent = t.producer_offset > 0 ? String(lag) : '—';
      lagEl.className   = 'a-lag ' + (t.producer_offset > 0 ? lagClass(lag) : 'lag-ok');
    }
  }
}
```

### Step 2: Replace `fetchStatus` to co-fetch both endpoints

Find the existing `fetchStatus` function:

```javascript
async function fetchStatus() {
  try {
    const r = await fetch(`${API}/status`);
    update(await r.json());
  } catch(e) {}
}
```

Replace it with:

```javascript
async function fetchStatus() {
  try {
    const [sr, mr] = await Promise.all([
      fetch(`${API}/status`),
      fetch(`${API}/kafka-meta`),
    ]);
    update(await sr.json());
    updateAnatomy(await mr.json());
  } catch(e) {}
}
```

### Step 3: Verify

Restart the stack and open the dashboard. After `docker compose up --build -d`:
- Before Start is clicked: topic offsets show `—`, lag shows `—`
- After clicking Start and waiting ~5s: offsets should start incrementing, lag should show a small number (0–5)
- Hover over every `ⓘ` tooltip to confirm they render correctly

### Step 4: Commit

```bash
git add dashboard/index.html
git commit -m "feat(dashboard): add updateAnatomy() JS and co-poll /kafka-meta with /status"
```

---

## Task 5: Dashboard — Start button readiness UX

**Files:**
- Modify: `dashboard/index.html`

### Step 1: Add Kafka readiness poller

Inside the `<script>` block, find the line:

```javascript
const API = 'http://localhost:8000';
let polling = null, maxEps = 1;
```

Replace it with:

```javascript
const API = 'http://localhost:8000';
let polling = null, maxEps = 1, kafkaReady = false, readyPoller = null;
```

### Step 2: Add `enableStartButton()` and `checkKafkaReady()` functions

Immediately after the `let polling = null...` line, add:

```javascript
function enableStartButton() {
  const btn  = document.getElementById('btn-start');
  const wrap = document.getElementById('start-wrap');
  btn.disabled = false;
  wrap.dataset.tip = 'Click to start producing events to Kafka';
}

async function checkKafkaReady() {
  try {
    const r    = await fetch(`${API}/kafka-meta`);
    const meta = await r.json();
    updateAnatomy(meta);
    if (!kafkaReady && meta.broker && meta.broker.status === 'ready') {
      kafkaReady = true;
      enableStartButton();
      clearInterval(readyPoller);
    }
  } catch(e) {}
}
```

### Step 3: Replace `window.onload`

Find the existing line at the bottom of the `<script>` block:

```javascript
window.onload = fetchStatus;
```

Replace it with:

```javascript
window.onload = function() {
  fetchStatus();
  // Poll for Kafka readiness every 3s until broker is up
  readyPoller = setInterval(checkKafkaReady, 3000);
  checkKafkaReady(); // immediate first check
};
```

### Step 4: Verify end-to-end student flow

Full test sequence:

1. `docker compose up --build -d`
2. Open `http://localhost:3001`
3. **Confirm:** `▶ Start Collection` is disabled; hovering shows *"Kafka broker is still starting up — usually ready in 20–30 seconds"*; broker dot is amber/pulsing
4. Wait ~30s. **Confirm:** button enables automatically without page refresh; broker dot turns green; tooltip changes to *"Click to start producing events to Kafka"*
5. Click `▶ Start Collection`. **Confirm:** dot turns bright green + "Live", offsets start incrementing in Kafka Anatomy, lag stays low (≤5), velocity meters show events/sec
6. Hover all `ⓘ` icons. **Confirm:** tooltips are readable and don't overflow the card
7. Click `■ Stop`. **Confirm:** collection stops, offsets freeze, lag drains to 0 as consumer catches up

### Step 5: Commit

```bash
git add dashboard/index.html
git commit -m "feat(dashboard): add Kafka readiness poller and smart Start button UX"
```

---

## Final verification

```bash
# Confirm all containers are healthy
docker compose ps

# Confirm /kafka-meta returns full data after start
curl -s http://localhost:8000/kafka-meta | python3 -m json.tool

# Confirm collector logs show no errors
docker compose logs collector --tail=30

# Confirm consumer is writing to ClickHouse
docker compose exec clickhouse clickhouse-client \
  --query "SELECT source, count() FROM velocity_lab.events GROUP BY source"
```

Expected: all 3 sources have non-zero row counts after ~60 seconds of collection.
