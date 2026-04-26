CREATE DATABASE IF NOT EXISTS pfe_db;

CREATE TABLE IF NOT EXISTS pfe_db.t_security (
    event_id String, device_id String, ts DateTime, program String, tag String,
    action String, status String, srcip String, dstip String, srcport UInt16, dstport UInt16,
    sentbyte UInt64, rcvdbyte UInt64, total_bytes UInt64, latency Float32,
    severity UInt8, severity_label String, is_anomaly UInt8, message String,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree() PARTITION BY toYYYYMM(ts) ORDER BY (ts, device_id) SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pfe_db.kafka_security_queue (
    event_id String, device_id String, ts String, program String, tag String,
    action String, status String, srcip String, dstip String, srcport UInt16, dstport UInt16,
    sentbyte UInt64, rcvdbyte UInt64, total_bytes UInt64, latency Float32,
    severity UInt8, severity_label String, is_anomaly UInt8, message String, ingested_at String
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'fg_security_events',
kafka_group_name = 'ch_security_consumer', kafka_format = 'JSONEachRow', kafka_max_block_size = 65536;

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_security TO pfe_db.t_security AS
SELECT event_id, device_id, coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
program, tag, action, status, srcip, dstip, srcport, dstport, sentbyte, rcvdbyte,
total_bytes, latency, severity, severity_label, is_anomaly, message FROM pfe_db.kafka_security_queue;

CREATE TABLE IF NOT EXISTS pfe_db.t_traffic (
    event_id String, device_id String, ts DateTime, program String, tag String,
    action String, status String, srcip String, dstip String, srcport UInt16, dstport UInt16,
    sentbyte UInt64, rcvdbyte UInt64, total_bytes UInt64, latency Float32,
    severity_label String, message String, ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree() PARTITION BY toYYYYMM(ts) ORDER BY (ts, device_id) SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pfe_db.kafka_traffic_queue (
    event_id String, device_id String, ts String, program String, tag String,
    action String, status String, srcip String, dstip String, srcport UInt16, dstport UInt16,
    sentbyte UInt64, rcvdbyte UInt64, total_bytes UInt64, latency Float32,
    severity_label String, message String, ingested_at String
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'fg_traffic_flow',
kafka_group_name = 'ch_traffic_consumer', kafka_format = 'JSONEachRow', kafka_max_block_size = 65536;

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_traffic TO pfe_db.t_traffic AS
SELECT event_id, device_id, coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
program, tag, action, status, srcip, dstip, srcport, dstport, sentbyte, rcvdbyte,
total_bytes, latency, severity_label, message FROM pfe_db.kafka_traffic_queue;

CREATE TABLE IF NOT EXISTS pfe_db.t_performance (
    event_id String, device_id String, ts DateTime, srcip String, dstip String,
    srcport UInt16, dstport UInt16, sentbyte UInt64, rcvdbyte UInt64, total_bytes UInt64,
    latency Float32, severity UInt8, severity_label String, is_anomaly UInt8, message String,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree() PARTITION BY toYYYYMM(ts) ORDER BY (ts, device_id) SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pfe_db.kafka_performance_queue (
    event_id String, device_id String, ts String, srcip String, dstip String,
    srcport UInt16, dstport UInt16, sentbyte UInt64, rcvdbyte UInt64, total_bytes UInt64,
    latency Float32, severity UInt8, severity_label String, is_anomaly UInt8, message String, ingested_at String
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'fg_performance',
kafka_group_name = 'ch_performance_consumer', kafka_format = 'JSONEachRow', kafka_max_block_size = 65536;

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_performance TO pfe_db.t_performance AS
SELECT event_id, device_id, coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
srcip, dstip, srcport, dstport, sentbyte, rcvdbyte, total_bytes, latency,
severity, severity_label, is_anomaly, message FROM pfe_db.kafka_performance_queue;

CREATE TABLE IF NOT EXISTS pfe_db.t_devices (
    device_id String, program String, tag String, first_seen DateTime DEFAULT now(),
    last_seen DateTime DEFAULT now(), total_events UInt64 DEFAULT 1, ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(last_seen) ORDER BY device_id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pfe_db.kafka_devices_queue (
    device_id String, program String, tag String, ingested_at String
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'fg_device_inventory',
kafka_group_name = 'ch_devices_consumer', kafka_format = 'JSONEachRow', kafka_max_block_size = 32768;

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_devices TO pfe_db.t_devices AS
SELECT device_id, program, tag, now() AS first_seen, now() AS last_seen, 1 AS total_events
FROM pfe_db.kafka_devices_queue WHERE length(device_id) > 0;

CREATE TABLE IF NOT EXISTS pfe_db.t_raw_events (
    event_id String, device_id String, ts DateTime, program String, tag String,
    action String, status String, srcip String, dstip String, srcport UInt16, dstport UInt16,
    sentbyte UInt64, rcvdbyte UInt64, total_bytes UInt64, latency Float32,
    severity UInt8, severity_label String, is_anomaly UInt8, message String,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree() PARTITION BY toYYYYMM(ts) ORDER BY (ts, device_id) SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pfe_db.kafka_raw_queue (
    event_id String, device_id String, ts String, program String, tag String,
    action String, status String, srcip String, dstip String, srcport UInt16, dstport UInt16,
    sentbyte UInt64, rcvdbyte UInt64, total_bytes UInt64, latency Float32,
    severity UInt8, severity_label String, is_anomaly UInt8, message String, ingested_at String
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'fg_raw_all',
kafka_group_name = 'ch_raw_consumer', kafka_format = 'JSONEachRow', kafka_max_block_size = 65536;

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_raw_events TO pfe_db.t_raw_events AS
SELECT event_id, device_id, coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
program, tag, action, status, srcip, dstip, srcport, dstport, sentbyte, rcvdbyte,
total_bytes, latency, severity, severity_label, is_anomaly, message FROM pfe_db.kafka_raw_queue;

CREATE VIEW IF NOT EXISTS pfe_db.v_top_threat_ips AS
SELECT srcip, count() AS nb_anomalies, max(severity) AS max_severity, max(ts) AS derniere_alerte
FROM pfe_db.t_security WHERE is_anomaly = 1 GROUP BY srcip ORDER BY nb_anomalies DESC LIMIT 50;

CREATE VIEW IF NOT EXISTS pfe_db.v_severity_per_hour AS
SELECT toStartOfHour(ts) AS heure, severity_label, count() AS nb_events
FROM pfe_db.t_raw_events GROUP BY heure, severity_label ORDER BY heure DESC;
