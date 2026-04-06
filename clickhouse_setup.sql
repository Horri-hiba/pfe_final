-- ============================================================
-- ClickHouse — Nouvelle architecture 5 topics
-- Fortinet FortiGate logs — 500 000 lignes
--
-- Tables :
--   t_security    ← fg_security_events   (anomalies, failures)
--   t_traffic     ← fg_traffic_flow      (trafic légitime)
--   t_performance ← fg_performance       (métriques latence/bytes/SD-WAN)
--   t_devices     ← fg_device_inventory  (inventaire équipements — dédupliqué)
--   t_raw_events  ← fg_raw_all           (audit complet 100%)
--
-- + Vues analytiques prêtes pour Grafana
-- ============================================================

CREATE DATABASE IF NOT EXISTS pfe_db;

-- ============================================================
-- TABLE 1 : t_security  (fg_security_events)
-- ============================================================
CREATE TABLE IF NOT EXISTS pfe_db.t_security
(
    event_id        String,
    device_id       String,
    ts              DateTime  DEFAULT now(),
    eventtime       String,
    message         String,
    program         String,
    tag             String,
    event_type      String,
    subtype         String,
    action          String,
    status          String,
    srcip           String,
    dstip           String,
    srcport         UInt16,
    dstport         UInt16,
    proto           UInt8,
    service         String,
    policyname      String,
    poluuid         String,
    sentbyte        UInt64,
    rcvdbyte        UInt64,
    total_bytes     UInt64,
    latency         Float32,
    sessionid       String,
    devname         String,
    devtype         String,
    srchwvendor     String,
    srcmac          String,
    severity        UInt8,
    severity_label  String,
    is_anomaly      UInt8,
    ingested_at     DateTime  DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (device_id, srcip, ts)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pfe_db.kafka_security_queue
(
    event_id        String,
    device_id       String,
    ts              String,
    eventtime       String,
    message         String,
    program         String,
    tag             String,
    event_type      String,
    subtype         String,
    action          String,
    status          String,
    srcip           String,
    dstip           String,
    srcport         UInt16,
    dstport         UInt16,
    proto           UInt8,
    service         String,
    policyname      String,
    poluuid         String,
    sentbyte        UInt64,
    rcvdbyte        UInt64,
    total_bytes     UInt64,
    latency         Float32,
    sessionid       String,
    devname         String,
    devtype         String,
    srchwvendor     String,
    srcmac          String,
    severity        UInt8,
    severity_label  String,
    is_anomaly      UInt8,
    ingested_at     String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list    = 'kafka:9092',
    kafka_topic_list     = 'fg_security_events',
    kafka_group_name     = 'ch_security_consumer',
    kafka_format         = 'JSONEachRow',
    kafka_max_block_size = 65536;

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_security
TO pfe_db.t_security AS
SELECT
    event_id,
    device_id,
    coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
    eventtime,
    message,
    program,
    tag,
    event_type,
    subtype,
    action,
    status,
    srcip,
    dstip,
    srcport,
    dstport,
    proto,
    service,
    policyname,
    poluuid,
    sentbyte,
    rcvdbyte,
    total_bytes,
    latency,
    sessionid,
    devname,
    devtype,
    srchwvendor,
    srcmac,
    severity,
    severity_label,
    is_anomaly
FROM pfe_db.kafka_security_queue;

-- ============================================================
-- TABLE 2 : t_traffic  (fg_traffic_flow)
-- CORRIGÉ : latency ajouté partout
-- ============================================================
CREATE TABLE IF NOT EXISTS pfe_db.t_traffic
(
    event_id        String,
    device_id       String,
    ts              DateTime  DEFAULT now(),
    message         String,
    program         String,
    tag             String,
    event_type      String,
    subtype         String,
    action          String,
    status          String,
    srcip           String,
    dstip           String,
    srcport         UInt16,
    dstport         UInt16,
    proto           UInt8,
    service         String,
    policyname      String,
    sentbyte        UInt64,
    rcvdbyte        UInt64,
    total_bytes     UInt64,
    latency         Float32,
    devname         String,
    devtype         String,
    srchwvendor     String,
    severity_label  String,
    ingested_at     DateTime  DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, device_id, subtype)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pfe_db.kafka_traffic_queue
(
    event_id        String,
    device_id       String,
    ts              String,
    message         String,
    program         String,
    tag             String,
    event_type      String,
    subtype         String,
    action          String,
    status          String,
    srcip           String,
    dstip           String,
    srcport         UInt16,
    dstport         UInt16,
    proto           UInt8,
    service         String,
    policyname      String,
    sentbyte        UInt64,
    rcvdbyte        UInt64,
    total_bytes     UInt64,
    latency         Float32,
    devname         String,
    devtype         String,
    srchwvendor     String,
    severity_label  String,
    ingested_at     String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list    = 'kafka:9092',
    kafka_topic_list     = 'fg_traffic_flow',
    kafka_group_name     = 'ch_traffic_consumer',
    kafka_format         = 'JSONEachRow',
    kafka_max_block_size = 65536;

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_traffic
TO pfe_db.t_traffic AS
SELECT
    event_id,
    device_id,
    coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
    message,
    program,
    tag,
    event_type,
    subtype,
    action,
    status,
    srcip,
    dstip,
    srcport,
    dstport,
    proto,
    service,
    policyname,
    sentbyte,
    rcvdbyte,
    total_bytes,
    latency,
    devname,
    devtype,
    srchwvendor,
    severity_label
FROM pfe_db.kafka_traffic_queue;

-- ============================================================
-- TABLE 3 : t_performance  (fg_performance)
-- ============================================================
CREATE TABLE IF NOT EXISTS pfe_db.t_performance
(
    event_id        String,
    device_id       String,
    ts              DateTime  DEFAULT now(),
    event_type      String,
    subtype         String,
    srcip           String,
    dstip           String,
    proto           UInt8,
    service         String,
    policyname      String,
    sentbyte        UInt64,
    rcvdbyte        UInt64,
    total_bytes     UInt64,
    latency         Float32,
    sessionid       String,
    vwlid           UInt8,
    vwlquality      String,
    devname         String,
    devtype         String,
    srchwvendor     String,
    severity        UInt8,
    is_anomaly      UInt8,
    ingested_at     DateTime  DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, device_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pfe_db.kafka_performance_queue
(
    event_id        String,
    device_id       String,
    ts              String,
    event_type      String,
    subtype         String,
    srcip           String,
    dstip           String,
    proto           UInt8,
    service         String,
    policyname      String,
    sentbyte        UInt64,
    rcvdbyte        UInt64,
    total_bytes     UInt64,
    latency         Float32,
    sessionid       String,
    vwlid           UInt8,
    vwlquality      String,
    devname         String,
    devtype         String,
    srchwvendor     String,
    severity        UInt8,
    is_anomaly      UInt8,
    ingested_at     String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list    = 'kafka:9092',
    kafka_topic_list     = 'fg_performance',
    kafka_group_name     = 'ch_performance_consumer',
    kafka_format         = 'JSONEachRow',
    kafka_max_block_size = 65536;

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_performance
TO pfe_db.t_performance AS
SELECT
    event_id,
    device_id,
    coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
    event_type,
    subtype,
    srcip,
    dstip,
    proto,
    service,
    policyname,
    sentbyte,
    rcvdbyte,
    total_bytes,
    latency,
    sessionid,
    vwlid,
    vwlquality,
    devname,
    devtype,
    srchwvendor,
    severity,
    is_anomaly
FROM pfe_db.kafka_performance_queue;

-- ============================================================
-- TABLE 4 : t_devices  (fg_device_inventory)
-- ============================================================
CREATE TABLE IF NOT EXISTS pfe_db.t_devices
(
    device_id       String,
    devname         String,
    devtype         String,
    srchwvendor     String,
    srcmac          String,
    first_seen      DateTime  DEFAULT now(),
    last_seen       DateTime  DEFAULT now(),
    total_events    UInt64    DEFAULT 1,
    ingested_at     DateTime  DEFAULT now()
)
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY device_id
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pfe_db.kafka_devices_queue
(
    device_id       String,
    devname         String,
    devtype         String,
    srchwvendor     String,
    srcmac          String,
    ingested_at     String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list    = 'kafka:9092',
    kafka_topic_list     = 'fg_device_inventory',
    kafka_group_name     = 'ch_devices_consumer',
    kafka_format         = 'JSONEachRow',
    kafka_max_block_size = 32768;

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_devices
TO pfe_db.t_devices AS
SELECT
    device_id,
    devname,
    devtype,
    srchwvendor,
    srcmac,
    now() AS first_seen,
    now() AS last_seen,
    1     AS total_events
FROM pfe_db.kafka_devices_queue
WHERE length(device_id) > 0 AND length(devtype) > 0;

-- ============================================================
-- TABLE 5 : t_raw_events  (fg_raw_all)
-- ============================================================
CREATE TABLE IF NOT EXISTS pfe_db.t_raw_events
(
    event_id        String,
    device_id       String,
    ts              DateTime  DEFAULT now(),
    eventtime       String,
    message         String,
    program         String,
    tag             String,
    seq             String,
    facility        UInt8,
    event_type      String,
    subtype         String,
    action          String,
    status          String,
    srcip           String,
    dstip           String,
    srcport         UInt16,
    dstport         UInt16,
    proto           UInt8,
    service         String,
    policyname      String,
    policytype      String,
    poluuid         String,
    sentbyte        UInt64,
    rcvdbyte        UInt64,
    total_bytes     UInt64,
    latency         Float32,
    sessionid       String,
    vwlid           UInt8,
    vwlquality      String,
    devname         String,
    devtype         String,
    srchwvendor     String,
    srcmac          String,
    severity        UInt8,
    severity_label  String,
    is_anomaly      UInt8,
    ingested_at     DateTime  DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, device_id, event_type)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pfe_db.kafka_raw_queue
(
    event_id        String,
    device_id       String,
    ts              String,
    eventtime       String,
    message         String,
    program         String,
    tag             String,
    seq             String,
    facility        UInt8,
    event_type      String,
    subtype         String,
    action          String,
    status          String,
    srcip           String,
    dstip           String,
    srcport         UInt16,
    dstport         UInt16,
    proto           UInt8,
    service         String,
    policyname      String,
    policytype      String,
    poluuid         String,
    sentbyte        UInt64,
    rcvdbyte        UInt64,
    total_bytes     UInt64,
    latency         Float32,
    sessionid       String,
    vwlid           UInt8,
    vwlquality      String,
    devname         String,
    devtype         String,
    srchwvendor     String,
    srcmac          String,
    severity        UInt8,
    severity_label  String,
    is_anomaly      UInt8,
    ingested_at     String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list    = 'kafka:9092',
    kafka_topic_list     = 'fg_raw_all',
    kafka_group_name     = 'ch_raw_consumer',
    kafka_format         = 'JSONEachRow',
    kafka_max_block_size = 65536;

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_raw_events
TO pfe_db.t_raw_events AS
SELECT
    event_id,
    device_id,
    coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
    eventtime,
    message,
    program,
    tag,
    seq,
    facility,
    event_type,
    subtype,
    action,
    status,
    srcip,
    dstip,
    srcport,
    dstport,
    proto,
    service,
    policyname,
    policytype,
    poluuid,
    sentbyte,
    rcvdbyte,
    total_bytes,
    latency,
    sessionid,
    vwlid,
    vwlquality,
    devname,
    devtype,
    srchwvendor,
    srcmac,
    severity,
    severity_label,
    is_anomaly
FROM pfe_db.kafka_raw_queue;

-- ============================================================
-- VUES ANALYTIQUES — directement utilisables dans Grafana
-- ============================================================

CREATE VIEW IF NOT EXISTS pfe_db.v_top_threat_ips AS
SELECT
    srcip,
    count()                     AS nb_anomalies,
    max(severity)               AS max_severity,
    groupUniqArray(action)      AS actions_vues,
    groupUniqArray(service)     AS services_cibles,
    max(ts)                     AS derniere_alerte
FROM pfe_db.t_security
WHERE is_anomaly = 1
GROUP BY srcip
ORDER BY nb_anomalies DESC
LIMIT 50;

CREATE VIEW IF NOT EXISTS pfe_db.v_device_summary AS
SELECT
    r.device_id,
    d.devtype,
    d.srchwvendor,
    d.srcmac,
    count()                          AS total_events,
    sum(r.is_anomaly)                AS total_anomalies,
    round(avg(r.latency), 2)         AS avg_latency_ms,
    max(r.latency)                   AS max_latency_ms,
    sum(r.total_bytes)               AS total_bytes,
    max(r.ts)                        AS last_seen
FROM pfe_db.t_raw_events r
LEFT JOIN pfe_db.t_devices d ON r.device_id = d.device_id
GROUP BY r.device_id, d.devtype, d.srchwvendor, d.srcmac
ORDER BY total_events DESC;

CREATE VIEW IF NOT EXISTS pfe_db.v_severity_per_hour AS
SELECT
    toStartOfHour(ts)   AS heure,
    severity_label,
    count()             AS nb_events
FROM pfe_db.t_raw_events
GROUP BY heure, severity_label
ORDER BY heure;

CREATE VIEW IF NOT EXISTS pfe_db.v_high_latency AS
SELECT
    ts,
    device_id,
    srcip,
    dstip,
    service,
    policyname,
    latency,
    vwlid,
    vwlquality,
    devtype,
    srchwvendor
FROM pfe_db.t_performance
WHERE latency > 20.0
ORDER BY latency DESC;

-- CORRIGÉ : latency maintenant disponible dans t_traffic
CREATE VIEW IF NOT EXISTS pfe_db.v_traffic_by_protocol AS
SELECT
    toStartOfHour(ts)       AS heure,
    subtype,
    count()                 AS nb_sessions,
    sum(total_bytes)        AS bytes_total,
    round(avg(latency), 2)  AS avg_latency_ms
FROM pfe_db.t_traffic
GROUP BY heure, subtype
ORDER BY heure, bytes_total DESC;

CREATE VIEW IF NOT EXISTS pfe_db.v_anomaly_by_type AS
SELECT
    message                 AS anomaly_type,
    count()                 AS occurrences,
    sum(is_anomaly)         AS confirmed_anomalies,
    max(severity)           AS max_severity,
    max(ts)                 AS last_occurrence
FROM pfe_db.t_security
GROUP BY message
ORDER BY occurrences DESC;