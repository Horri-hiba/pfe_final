-- ============================================
-- NOUVELLE TABLE : Logs Réseau (data2)
-- ============================================

CREATE TABLE IF NOT EXISTS pfe_db.t_network_logs (
    event_id String,
    device_id String,
    ts DateTime,
    program String,
    log_type String,
    message String,
    src_ip String,
    dst_ip String,
    mac_address String,
    raw_length UInt32,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, device_id, log_type)
SETTINGS index_granularity = 8192;

-- ============================================
-- TABLE KAFKA POUR data2
-- ============================================

CREATE TABLE IF NOT EXISTS pfe_db.kafka_network_queue (
    event_id String,
    device_id String,
    ts String,
    program String,
    log_type String,
    message String,
    src_ip String,
    dst_ip String,
    mac_address String,
    raw_length UInt32,
    ingested_at String
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'fg_network_logs',
    kafka_group_name = 'ch_network_consumer',
    kafka_format = 'JSONEachRow',
    kafka_max_block_size = 65536;

-- ============================================
-- MATERIALIZED VIEW : data2 → t_network_logs
-- ============================================

CREATE MATERIALIZED VIEW IF NOT EXISTS pfe_db.mv_network_logs
TO pfe_db.t_network_logs AS
SELECT 
    event_id,
    device_id,
    coalesce(parseDateTimeBestEffortOrNull(ts), now()) AS ts,
    program,
    log_type,
    message,
    src_ip,
    dst_ip,
    mac_address,
    raw_length
FROM pfe_db.kafka_network_queue;

-- ============================================
-- VUES ANALYTIQUES pour data2
-- ============================================

CREATE VIEW IF NOT EXISTS pfe_db.v_network_log_types AS
SELECT 
    log_type,
    count() AS nb_events,
    max(ts) AS last_seen,
    uniqExact(device_id) AS nb_devices
FROM pfe_db.t_network_logs
GROUP BY log_type
ORDER BY nb_events DESC;

CREATE VIEW IF NOT EXISTS pfe_db.v_dhcp_activity AS
SELECT 
    toStartOfHour(ts) AS heure,
    count() AS nb_dhcp_events,
    uniqExact(src_ip) AS nb_ips,
    groupArrayDistinct(device_id) AS devices
FROM pfe_db.t_network_logs
WHERE log_type = 'dhcp'
GROUP BY heure
ORDER BY heure DESC;

CREATE VIEW IF NOT EXISTS pfe_db.v_wlan_alerts AS
SELECT 
    ts,
    device_id,
    program,
    log_type,
    message,
    mac_address
FROM pfe_db.t_network_logs
WHERE log_type IN ('ip_conflict', 'user_offline', 'wlan')
ORDER BY ts DESC
LIMIT 100;

CREATE VIEW IF NOT EXISTS pfe_db.v_global_events AS
SELECT 
    'security' AS source,
    event_id,
    device_id,
    ts,
    program,
    severity_label AS category,
    message
FROM pfe_db.t_security
UNION ALL
SELECT 
    'network' AS source,
    event_id,
    device_id,
    ts,
    program,
    log_type AS category,
    message
FROM pfe_db.t_network_logs
ORDER BY ts DESC
LIMIT 1000;
