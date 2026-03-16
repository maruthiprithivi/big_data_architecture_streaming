# Student-Friendly Dashboard Design
**Date:** 2026-03-16
**Project:** IS459 · Big Data Architecture · Week 7 — Velocity Showdown
**Status:** Approved

---

## Goal

Make the project immediately accessible to students with zero terminal knowledge beyond one command. Students open a browser, click Start, and see live streaming data alongside Kafka internals — learning velocity and Kafka mechanics visually without reading documentation first.

---

## Architecture

```mermaid
graph TD
    subgraph Sources ["Public Data Sources"]
        S1[Solana RPC<br/>~2.5 slots/sec]
        S2[Mastodon SSE<br/>~1–3 posts/sec]
        S3[Wikipedia SSE<br/>~0.3 edits/sec]
    end

    subgraph Docker ["Docker Compose — one command"]
        K[Kafka Broker<br/>KRaft · port 9092]
        KUI[Kafka UI<br/>port 8080]
        CH[ClickHouse<br/>port 8123]

        subgraph Collector ["Collector · port 8000"]
            P[Producer]
            ADMIN[AIOKafkaAdminClient<br/>NEW — /kafka-meta]
        end

        CON[Consumer<br/>clickhouse-sink group]
        DASH[Dashboard nginx<br/>port 3001]
    end

    subgraph Browser ["Student Browser"]
        UI[Dashboard UI]
    end

    S1 & S2 & S3 --> P
    P -->|produce| K
    K --> KUI
    K --> CON
    CON -->|insert| CH
    ADMIN -.->|admin queries| K
    UI -->|GET /status| P
    UI -->|GET /kafka-meta NEW| ADMIN
    DASH --> UI
```

---

## Changes Required

### 1. Collector — `/kafka-meta` endpoint

**File:** `collector/main.py`

New `GET /kafka-meta` endpoint using `AIOKafkaAdminClient` (already available via `aiokafka`).

**Response shape:**
```json
{
  "broker": {
    "node_id": 1,
    "host": "kafka",
    "port": 9092,
    "mode": "KRaft",
    "status": "ready"
  },
  "topics": {
    "velocity.solana": {
      "partition": 0,
      "producer_offset": 1482,
      "consumer_offset": 1480,
      "lag": 2
    },
    "velocity.mastodon": {
      "partition": 0,
      "producer_offset": 347,
      "consumer_offset": 345,
      "lag": 2
    },
    "velocity.wikipedia": {
      "partition": 0,
      "producer_offset": 91,
      "consumer_offset": 90,
      "lag": 1
    }
  },
  "consumer_group": "clickhouse-sink"
}
```

**Implementation details:**
- `AIOKafkaAdminClient` fetches topic partition metadata → broker host/port/node_id
- `list_consumer_group_offsets("clickhouse-sink")` → consumer offsets per partition
- `AIOKafkaConsumer.end_offsets()` → latest (producer) offsets per partition
- Result cached in memory for 2 seconds — prevents per-request Kafka admin overhead
- Returns `"status": "connecting"` with zero offsets if Kafka is not yet reachable — safe to call before Start is clicked
- `broker.status` flips to `"ready"` once the admin client successfully connects

---

### 2. Dashboard — Kafka Anatomy Section

**File:** `dashboard/index.html`

New section inserted between the velocity meters and the existing bottom comparison panels.

```mermaid
graph LR
    subgraph KafkaAnatomy ["Kafka Anatomy Section"]
        B[Broker Card<br/>Node 1 · KRaft · :9092<br/>● connected]
        T1[velocity.solana<br/>Partition: 0<br/>Produced: ↑ offset<br/>Consumed: ↑ offset<br/>Lag: n]
        T2[velocity.mastodon<br/>Partition: 0<br/>Produced: ↑ offset<br/>Consumed: ↑ offset<br/>Lag: n]
        T3[velocity.wikipedia<br/>Partition: 0<br/>Produced: ↑ offset<br/>Consumed: ↑ offset<br/>Lag: n]
    end
    B --- T1 & T2 & T3
```

**Educational tooltips** — CSS-only `ⓘ` icons on every Kafka term:

| Term | Tooltip |
|---|---|
| Broker | The Kafka server. Stores messages and serves producers & consumers. |
| KRaft | No ZooKeeper needed — Kafka manages its own consensus since v3.3. |
| Partition | Topics are split into ordered partitions. 1 partition = 1 ordered log. |
| Produced offset | Every message gets a sequential number. This is how many have been written. |
| Consumed offset | Where the `clickhouse-sink` consumer group is up to. |
| Lag | Gap between produced and consumed. Low lag = consumer keeping up. |

**Visual rules:**
- Offsets formatted as `K`/`M` (e.g. `14.2K`) — prevents overflow
- Lag colour coding: ≤5 → green, 6–20 → amber, >20 → red
- Topic colours match existing scheme: teal (Solana) / amber (Mastodon) / sage (Wikipedia)
- Offsets animate/tick up on every 1s poll

---

### 3. Dashboard — Start Button UX

**File:** `dashboard/index.html`

```mermaid
stateDiagram-v2
    [*] --> KafkaStarting : page load
    KafkaStarting --> KafkaReady : /kafka-meta returns status=ready
    KafkaReady --> Collecting : student clicks ▶ Start Collection
    Collecting --> Stopped : student clicks ■ Stop
    Stopped --> Collecting : student clicks ▶ Start Collection

    KafkaStarting : Button disabled\nHover → "Kafka broker is still starting up —\nusually ready in 20–30 seconds"
    KafkaReady : Button enabled\nHover → "Click to start producing events to Kafka"
    Collecting : Button disabled\nDot → green pulse ● Live
    Stopped : Button enabled\nDot → grey
```

**Implementation:**
- On page load, poll `GET /kafka-meta` every 3 seconds
- `▶ Start Collection` button stays `disabled` until `broker.status === "ready"`
- CSS tooltip on the disabled button (no JS tooltip library)
- Once ready, button enables automatically — no page refresh needed
- Header status dot: grey (starting) → dim white (ready, not yet collecting) → green pulse (collecting)

---

## File Change Summary

| File | Change |
|---|---|
| `collector/main.py` | Add `GET /kafka-meta` endpoint + `AIOKafkaAdminClient` + 2s cache |
| `dashboard/index.html` | Add Kafka Anatomy section, tooltip styles, button readiness logic |

No new containers. No new dependencies. No changes to `docker-compose.yml`.

---

## Student Experience Flow

```mermaid
sequenceDiagram
    participant S as Student
    participant B as Browser
    participant D as Dashboard :3001
    participant C as Collector :8000
    participant K as Kafka :9092

    S->>B: docker compose up --build -d
    S->>B: open localhost:3001
    B->>C: GET /kafka-meta (every 3s)
    C->>K: admin connect attempt
    K-->>C: still starting...
    C-->>B: status: connecting
    Note over B: ▶ Start Collection disabled<br/>tooltip: "Kafka warming up…"

    K-->>C: ready
    C-->>B: status: ready
    Note over B: ▶ Start Collection enabled

    S->>B: clicks ▶ Start Collection
    B->>C: POST /start
    C->>K: produce velocity.solana/mastodon/wikipedia

    loop every 1 second
        B->>C: GET /status
        B->>C: GET /kafka-meta
        C-->>B: eps, totals, offsets, lag
        Note over B: Velocity meters + Kafka Anatomy update live
    end
```
