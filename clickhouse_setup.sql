-- ============================================================
-- ClickHouse : 4 tables + 4 Kafka Engines + 4 Materialized Views
-- ============================================================

CREATE DATABASE IF NOT EXISTS pfe_db;

-- ============================================================
-- TABLE 1 : Alertes réseau
-- ============================================================
CREATE TABLE IF NOT EXISTS pfe_db.t_alerts
(
    event_id        String,
    device_id       String,
    ts              DateTime DEFAULT now(),
    message         String,
    event_type      String,
    ref             String,
    severity        UInt8,
    severity_label  String,
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (device_id, ts);

CREATE TABLE IF NOT EXISTS pfe_db.kafka_alerts_queue
(
    event_id String, device_id String, ts String,
    message String, event_type String, ref String,
    extra String, severity UInt8, severity_label String,
    is_anomaly UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'kafka:9092',
    kafka_topic_list    = 'network_alerts',
    kafka_group_name    = 'ch_alerts_consumer',
    kafka_format        = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_alerts
TO pfe_db.t_alerts AS
SELECT
    event_id,
    device_id,
    coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
    message,
    event_type,
    ref,
    severity,
    severity_label
FROM pfe_db.kafka_alerts_queue;

-- ============================================================
-- TABLE 2 : Informations
-- ============================================================
CREATE TABLE IF NOT EXISTS pfe_db.t_info
(
    event_id        String,
    device_id       String,
    ts              DateTime DEFAULT now(),
    message         String,
    event_type      String,
    severity_label  String,
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (device_id, ts);

CREATE TABLE IF NOT EXISTS pfe_db.kafka_info_queue
(
    event_id String, device_id String, ts String,
    message String, event_type String, ref String,
    extra String, severity UInt8, severity_label String,
    is_anomaly UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'kafka:9092',
    kafka_topic_list    = 'network_info',
    kafka_group_name    = 'ch_info_consumer',
    kafka_format        = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_info
TO pfe_db.t_info AS
SELECT
    event_id,
    device_id,
    coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
    message,
    event_type,
    severity_label
FROM pfe_db.kafka_info_queue;

-- ============================================================
-- TABLE 3 : Métriques hardware
-- ============================================================
CREATE TABLE IF NOT EXISTS pfe_db.t_metrics
(
    event_id        String,
    device_id       String,
    ts              DateTime DEFAULT now(),
    message         String,
    event_type      String,
    ref             String,
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (device_id, event_type, ts);

CREATE TABLE IF NOT EXISTS pfe_db.kafka_metrics_queue
(
    event_id String, device_id String, ts String,
    message String, event_type String, ref String,
    extra String, severity UInt8, severity_label String,
    is_anomaly UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'kafka:9092',
    kafka_topic_list    = 'network_metrics',
    kafka_group_name    = 'ch_metrics_consumer',
    kafka_format        = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_metrics
TO pfe_db.t_metrics AS
SELECT
    event_id,
    device_id,
    coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
    message,
    event_type,
    ref
FROM pfe_db.kafka_metrics_queue;

-- ============================================================
-- TABLE 4 : Tous les événements
-- ============================================================
CREATE TABLE IF NOT EXISTS pfe_db.t_all_events
(
    event_id        String,
    device_id       String,
    ts              DateTime DEFAULT now(),
    message         String,
    event_type      String,
    ref             String,
    severity        UInt8,
    severity_label  String,
    is_anomaly      UInt8,
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (device_id, event_type, ts);

CREATE TABLE IF NOT EXISTS pfe_db.kafka_all_queue
(
    event_id String, device_id String, ts String,
    message String, event_type String, ref String,
    extra String, severity UInt8, severity_label String,
    is_anomaly UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'kafka:9092',
    kafka_topic_list    = 'network_all',
    kafka_group_name    = 'ch_all_consumer',
    kafka_format        = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_all_events
TO pfe_db.t_all_events AS
SELECT
    event_id,
    device_id,
    coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
    message,
    event_type,
    ref,
    severity,
    severity_label,
    is_anomaly
FROM pfe_db.kafka_all_queue;