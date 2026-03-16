-- Velocity Showdown — ClickHouse Schema
CREATE DATABASE IF NOT EXISTS velocity_lab;

CREATE TABLE IF NOT EXISTS velocity_lab.events
(
    source       LowCardinality(String),
    event_type   LowCardinality(String),
    event_time   DateTime64(3),
    collected_at DateTime64(3) DEFAULT now64(3),
    summary      String  CODEC(ZSTD(3)),
    metadata     String  CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY (source, toYYYYMMDD(event_time))
ORDER BY (source, event_time)
TTL event_time + INTERVAL 4 HOUR
SETTINGS index_granularity = 8192;

-- Sample queries for classroom exploration:
-- SELECT source, count() FROM velocity_lab.events GROUP BY source ORDER BY source;
-- SELECT source, count()/dateDiff('second', min(collected_at), max(collected_at)) AS avg_eps
--   FROM velocity_lab.events GROUP BY source;
-- SELECT JSONExtractString(metadata,'user') AS editor, count() AS edits
--   FROM velocity_lab.events WHERE source='wikipedia' GROUP BY editor ORDER BY edits DESC LIMIT 10;
