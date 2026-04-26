"""
Spark Batch Pipeline — FortiGate Network Logs
Architecture : Kafka → Bronze (raw) → Silver (cleaned) → Gold (aggregated)
Stockage     : Apache Iceberg on MinIO S3 via REST catalog
Requête      : Trino → Grafana

Optimisations clés pour 8GB RAM :
  1. Ecrire chaque étape sur disque (Iceberg) et relire les tables
  2. OverwritePartitions pour idempotence (pas de doublons)
  3. Pas de .cache() / pas de .repartition() agressif
  4. Compression ZSTD (meilleur ratio que "uncompressed")
  5. AQE (Adaptive Query Execution) activé
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    when, trim, count, avg, sum as spark_sum,
    max as spark_max, window, hour, dayofweek,
    coalesce, lit, countDistinct, percentile_approx
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, FloatType
)

# ── Configuration ────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = "http://minio1:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
CATALOG_URI      = "http://iceberg-rest:8181"
WAREHOUSE        = "s3://iceberg/"
KAFKA_SERVERS    = "kafka-batch:9092"
TOPICS           = "fg_raw_all,fg_security_events,fg_traffic_flow,fg_performance,fg_device_inventory"

# Schéma JSON aligné avec la sortie Logstash et le pipeline Vector real-time
MSG_SCHEMA = StructType([
    StructField("event_id",       StringType(), True),
    StructField("device_id",      StringType(), True),
    StructField("ts",             StringType(), True),
    StructField("eventtime",      StringType(), True),
    StructField("ingested_at",    StringType(), True),
    StructField("message",        StringType(), True),
    StructField("tag",            StringType(), True),
    StructField("program",        StringType(), True),
    StructField("seq",            StringType(), True),
    StructField("facility",       IntegerType(), True),
    StructField("event_type",     StringType(), True),
    StructField("subtype",        StringType(), True),
    StructField("action",         StringType(), True),
    StructField("status",         StringType(), True),
    StructField("srcip",          StringType(), True),
    StructField("dstip",          StringType(), True),
    StructField("srcport",        IntegerType(), True),
    StructField("dstport",        IntegerType(), True),
    StructField("proto",          IntegerType(), True),
    StructField("service",        StringType(), True),
    StructField("policyname",     StringType(), True),
    StructField("policytype",     StringType(), True),
    StructField("poluuid",        StringType(), True),
    StructField("sentbyte",       LongType(), True),
    StructField("rcvdbyte",       LongType(), True),
    StructField("total_bytes",    LongType(), True),
    StructField("latency",        FloatType(), True),
    StructField("sessionid",      StringType(), True),
    StructField("vwlid",          IntegerType(), True),
    StructField("vwlquality",     StringType(), True),
    StructField("devname",        StringType(), True),
    StructField("devtype",        StringType(), True),
    StructField("srchwvendor",    StringType(), True),
    StructField("srcmac",         StringType(), True),
    StructField("severity",       IntegerType(), True),
    StructField("severity_label", StringType(), True),
    StructField("is_anomaly",     IntegerType(), True),
])


def build_spark():
    """
    Construit une SparkSession optimisée pour une machine 8GB.
    Toutes les configs catalogue sont ici (pas de duplication docker-compose).
    """
    return (
        SparkSession.builder
        .appName("FortiGateBatchPipeline-Iceberg")
        # ── Tuning mémoire 8GB ──
        .config("spark.driver.memory", "1536m")
        .config("spark.executor.memory", "1536m")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "512m")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # ── Iceberg REST Catalog ──
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", CATALOG_URI)
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.iceberg.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
        .config("spark.sql.iceberg.compression-codec", "zstd")
        # ── S3A fallback (si accès direct Hadoop) ──
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def ensure_tables(spark):
    """Crée les namespaces et tables Iceberg si inexistants."""
    for ns in ["bronze", "silver", "gold"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{ns}")

    # ── BRONZE : ingestion brute (tous champs) ─────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.fg_events_raw (
            ingested_at     TIMESTAMP,
            kafka_topic     STRING,
            kafka_offset    BIGINT,
            event_id        STRING,
            device_id       STRING,
            ts              STRING,
            eventtime       STRING,
            message         STRING,
            tag             STRING,
            program         STRING,
            seq             STRING,
            facility        INT,
            event_type      STRING,
            subtype         STRING,
            action          STRING,
            status          STRING,
            srcip           STRING,
            dstip           STRING,
            srcport         INT,
            dstport         INT,
            proto           INT,
            service         STRING,
            policyname      STRING,
            policytype      STRING,
            poluuid         STRING,
            sentbyte        BIGINT,
            rcvdbyte        BIGINT,
            total_bytes     BIGINT,
            latency         FLOAT,
            sessionid       STRING,
            vwlid           INT,
            vwlquality      STRING,
            devname         STRING,
            devtype         STRING,
            srchwvendor     STRING,
            srcmac          STRING,
            severity        INT,
            severity_label  STRING,
            is_anomaly      INT
        ) USING iceberg
        PARTITIONED BY (days(ingested_at))
        TBLPROPERTIES ('write_compression'='zstd')
    """)

    # ── SILVER : nettoyé et typé (miroir ClickHouse real-time) ─────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.fg_events_clean (
            ingested_at     TIMESTAMP,
            event_time      TIMESTAMP,
            kafka_topic     STRING,
            event_id        STRING,
            device_id       STRING,
            message         STRING,
            tag             STRING,
            program         STRING,
            event_type      STRING,
            subtype         STRING,
            action          STRING,
            status          STRING,
            srcip           STRING,
            dstip           STRING,
            srcport         INT,
            dstport         INT,
            proto           INT,
            service         STRING,
            policyname      STRING,
            policytype      STRING,
            poluuid         STRING,
            sentbyte        BIGINT,
            rcvdbyte        BIGINT,
            total_bytes     BIGINT,
            latency         FLOAT,
            sessionid       STRING,
            vwlid           INT,
            vwlquality      STRING,
            devname         STRING,
            devtype         STRING,
            srchwvendor     STRING,
            srcmac          STRING,
            severity        INT,
            severity_label  STRING,
            is_anomaly      INT,
            hour_of_day     INT,
            day_of_week     INT,
            proto_label     STRING,
            latency_bucket  STRING,
            is_vpn_failure  INT
        ) USING iceberg
        PARTITIONED BY (days(event_time), bucket(16, device_id))
        TBLPROPERTIES ('write_compression'='zstd')
    """)

    # ── GOLD : KPIs horaires (partitionné pour Trino/Grafana) ──────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.fg_kpi_hourly (
            window_start    TIMESTAMP,
            window_end      TIMESTAMP,
            event_type      STRING,
            severity_label  STRING,
            total_events    BIGINT,
            anomaly_count   BIGINT,
            critical_count  BIGINT,
            error_count     BIGINT,
            warning_count   BIGINT,
            avg_latency_ms  DOUBLE,
            max_latency_ms  DOUBLE,
            total_bytes     BIGINT,
            avg_bytes       DOUBLE,
            unique_devices  BIGINT,
            unique_srcip    BIGINT
        ) USING iceberg
        PARTITIONED BY (days(window_start))
        TBLPROPERTIES ('write_compression'='zstd')
    """)

    # ── GOLD : inventaire devices ──────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.fg_device_inventory (
            device_id       STRING,
            devname         STRING,
            devtype         STRING,
            srchwvendor     STRING,
            srcmac          STRING,
            total_events    BIGINT,
            total_anomalies BIGINT,
            avg_latency_ms  DOUBLE,
            max_latency_ms  DOUBLE,
            total_bytes     BIGINT,
            last_seen       TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('write_compression'='zstd')
    """)

    # ── GOLD : top IPs suspectes ─────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.fg_top_threat_ips (
            srcip           STRING,
            nb_anomalies    BIGINT,
            max_severity    INT,
            total_events    BIGINT,
            last_alert      TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('write_compression'='zstd')
    """)

    # ── GOLD : SD-WAN performance (partitionné) ──────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.fg_sdwan_performance (
            window_start        TIMESTAMP,
            vwlid               INT,
            avg_latency_ms      DOUBLE,
            max_latency_ms      DOUBLE,
            p95_latency_ms      DOUBLE,
            degraded_sessions   BIGINT,
            critical_sessions   BIGINT,
            total_sessions      BIGINT,
            total_bytes         BIGINT
        ) USING iceberg
        PARTITIONED BY (days(window_start))
        TBLPROPERTIES ('write_compression'='zstd')
    """)

    # ── GOLD : trafic par politique ────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.fg_policy_traffic (
            policyname          STRING,
            policytype          STRING,
            total_sessions      BIGINT,
            blocked_sessions    BIGINT,
            total_bytes         BIGINT,
            avg_latency_ms      DOUBLE,
            unique_src_ips      BIGINT,
            unique_dst_ips      BIGINT
        ) USING iceberg
        TBLPROPERTIES ('write_compression'='zstd')
    """)

    # ── GOLD : stats protocoles par jour (partitionné) ───────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.fg_protocol_stats (
            day                 TIMESTAMP,
            tag                 STRING,
            total_sessions      BIGINT,
            anomaly_sessions    BIGINT,
            total_bytes         BIGINT,
            avg_latency_ms      DOUBLE,
            unique_sources      BIGINT
        ) USING iceberg
        PARTITIONED BY (days(day))
        TBLPROPERTIES ('write_compression'='zstd')
    """)

    print("[TABLES] Tables Iceberg bronze/silver/gold verifiees.")


def write_bronze(spark):
    """
    Étape 1 : Kafka → Bronze
    - Lit tous les offsets disponibles (earliest → latest)
    - Dédoublonne sur event_id (dans le micro-batch courant)
    - ECRASE les partitions existantes (idempotence totale)
    """
    print(f"[BRONZE] Lecture Kafka : {TOPICS}")
    raw_kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "500000")
        .option("kafka.max.partition.fetch.bytes", "1048576")
        .load()
    )

    total_kafka = raw_kafka.count()
    print(f"[BRONZE] {total_kafka} messages bruts extraits de Kafka")

    parsed = raw_kafka.withColumn(
        "data", from_json(col("value").cast("string"), MSG_SCHEMA)
    )

    df = parsed.select(
        current_timestamp().alias("ingested_at"),
        col("topic").alias("kafka_topic"),
        col("offset").cast("long").alias("kafka_offset"),
        col("data.event_id"),
        col("data.device_id"),
        col("data.ts"),
        col("data.eventtime"),
        col("data.message"),
        col("data.tag"),
        col("data.program"),
        col("data.seq"),
        col("data.facility"),
        col("data.event_type"),
        col("data.subtype"),
        col("data.action"),
        col("data.status"),
        col("data.srcip"),
        col("data.dstip"),
        col("data.srcport"),
        col("data.dstport"),
        col("data.proto"),
        col("data.service"),
        col("data.policyname"),
        col("data.policytype"),
        col("data.poluuid"),
        col("data.sentbyte"),
        col("data.rcvdbyte"),
        col("data.total_bytes"),
        col("data.latency"),
        col("data.sessionid"),
        col("data.vwlid"),
        col("data.vwlquality"),
        col("data.devname"),
        col("data.devtype"),
        col("data.srchwvendor"),
        col("data.srcmac"),
        col("data.severity"),
        col("data.severity_label"),
        col("data.is_anomaly"),
    ).filter(
        col("device_id").isNotNull() & (trim(col("device_id")) != "")
    ).dropDuplicates(["event_id"])

    # Idempotence : overwritePartitions remplace les partitions jour déjà existantes
    df.coalesce(4).writeTo("iceberg.bronze.fg_events_raw") \
        .using("iceberg") \
        .option("write_compression", "zstd") \
        .overwritePartitions()

    cnt = df.count()
    print(f"[BRONZE] {cnt} evenements dedupliques → iceberg.bronze.fg_events_raw")
    return cnt


def write_silver(spark):
    """
    Étape 2 : Bronze → Silver
    - Lit la table Bronze fraîchement écrite (libère la mémoire inter-étape)
    - Nettoie, type, enrichit
    - ECRASE les partitions Silver existantes
    """
    print("[SILVER] Lecture table Bronze...")
    bronze = spark.table("iceberg.bronze.fg_events_raw")

    df = bronze \
        .withColumn(
            "event_time",
            coalesce(
                to_timestamp(trim(col("ts")), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                to_timestamp(trim(col("ts")), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(trim(col("ts")), "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(trim(col("eventtime")), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                current_timestamp()
            )
        ) \
        .withColumn("hour_of_day", hour(col("event_time"))) \
        .withColumn("day_of_week", dayofweek(col("event_time"))) \
        .withColumn("proto_label",
            when(col("proto") == 6,  lit("TCP"))
            .when(col("proto") == 17, lit("UDP"))
            .when(col("proto") == 1,  lit("ICMP"))
            .otherwise(col("proto").cast("string"))
        ) \
        .withColumn("latency_bucket",
            when(col("latency") == 0,    lit("no_data"))
            .when(col("latency") < 5,    lit("excellent"))
            .when(col("latency") < 20,   lit("good"))
            .when(col("latency") < 50,   lit("degraded"))
            .otherwise(                   lit("critical"))
        ) \
        .withColumn("is_vpn_failure",
            when(
                col("message").rlike("(?i)IPsec|IKE|VPN") & (col("status") == "failed"),
                lit(1)
            ).otherwise(lit(0))
        ) \
        .select(
            col("ingested_at"),
            col("event_time"),
            col("kafka_topic"),
            trim(col("event_id")).alias("event_id"),
            trim(col("device_id")).alias("device_id"),
            trim(col("message")).alias("message"),
            trim(col("tag")).alias("tag"),
            trim(col("program")).alias("program"),
            trim(col("event_type")).alias("event_type"),
            trim(col("subtype")).alias("subtype"),
            trim(col("action")).alias("action"),
            trim(col("status")).alias("status"),
            trim(col("srcip")).alias("srcip"),
            trim(col("dstip")).alias("dstip"),
            coalesce(col("srcport"), lit(0)).alias("srcport"),
            coalesce(col("dstport"), lit(0)).alias("dstport"),
            coalesce(col("proto"), lit(0)).alias("proto"),
            trim(col("service")).alias("service"),
            trim(col("policyname")).alias("policyname"),
            trim(col("policytype")).alias("policytype"),
            trim(col("poluuid")).alias("poluuid"),
            coalesce(col("sentbyte"), lit(0)).alias("sentbyte"),
            coalesce(col("rcvdbyte"), lit(0)).alias("rcvdbyte"),
            coalesce(col("total_bytes"), lit(0)).alias("total_bytes"),
            coalesce(col("latency"), lit(0.0)).alias("latency"),
            trim(col("sessionid")).alias("sessionid"),
            coalesce(col("vwlid"), lit(0)).alias("vwlid"),
            trim(col("vwlquality")).alias("vwlquality"),
            trim(col("devname")).alias("devname"),
            trim(col("devtype")).alias("devtype"),
            trim(col("srchwvendor")).alias("srchwvendor"),
            trim(col("srcmac")).alias("srcmac"),
            coalesce(col("severity"), lit(1)).alias("severity"),
            trim(col("severity_label")).alias("severity_label"),
            coalesce(col("is_anomaly"), lit(0)).alias("is_anomaly"),
            col("hour_of_day"),
            col("day_of_week"),
            col("proto_label"),
            col("latency_bucket"),
            col("is_vpn_failure"),
        ).filter(col("event_time").isNotNull())

    df.coalesce(4).writeTo("iceberg.silver.fg_events_clean") \
        .using("iceberg") \
        .option("write_compression", "zstd") \
        .overwritePartitions()

    cnt = df.count()
    print(f"[SILVER] {cnt} evenements nettoyes → iceberg.silver.fg_events_clean")
    return cnt


def write_gold(spark):
    """
    Étape 3 : Silver → Gold
    - Lit la table Silver (pas de DataFrame en mémoire entre étapes)
    - Calcule 6 tables d'agrégats
    - Overwrite total (tables Gold petites, recalcul rapide et idempotent)
    """
    print("[GOLD] Lecture table Silver...")
    silver = spark.table("iceberg.silver.fg_events_clean")

    # ── 1) KPI horaires ────────────────────────────────────────────────────
    print("[GOLD] Calcul KPI horaires...")
    kpi = silver.groupBy(
        window(col("event_time"), "1 hour"),
        col("event_type"),
        col("severity_label"),
    ).agg(
        count("*").alias("total_events"),
        count(when(col("is_anomaly") == 1, 1)).alias("anomaly_count"),
        count(when(col("severity") >= 5, 1)).alias("critical_count"),
        count(when(col("severity") == 4, 1)).alias("error_count"),
        count(when(col("severity") == 3, 1)).alias("warning_count"),
        avg("latency").alias("avg_latency_ms"),
        spark_max("latency").alias("max_latency_ms"),
        spark_sum("total_bytes").alias("total_bytes"),
        avg("total_bytes").alias("avg_bytes"),
        countDistinct("device_id").alias("unique_devices"),
        countDistinct("srcip").alias("unique_srcip"),
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_type", "severity_label",
        "total_events", "anomaly_count", "critical_count",
        "error_count", "warning_count",
        "avg_latency_ms", "max_latency_ms",
        "total_bytes", "avg_bytes",
        "unique_devices", "unique_srcip",
    )

    kpi.coalesce(2).writeTo("iceberg.gold.fg_kpi_hourly") \
        .using("iceberg") \
        .option("write_compression", "zstd") \
        .overwritePartitions()
    print("[GOLD] fg_kpi_hourly OK")

    # ── 2) Inventaire devices ──────────────────────────────────────────────
    print("[GOLD] Calcul inventaire devices...")
    devices = silver.groupBy(
        "device_id", "devname", "devtype", "srchwvendor", "srcmac"
    ).agg(
        count("*").alias("total_events"),
        count(when(col("is_anomaly") == 1, 1)).alias("total_anomalies"),
        avg("latency").alias("avg_latency_ms"),
        spark_max("latency").alias("max_latency_ms"),
        spark_sum("total_bytes").alias("total_bytes"),
        spark_max("event_time").alias("last_seen"),
    ).filter(col("device_id").isNotNull())

    devices.coalesce(2).writeTo("iceberg.gold.fg_device_inventory") \
        .using("iceberg") \
        .option("write_compression", "zstd") \
        .overwritePartitions()
    print("[GOLD] fg_device_inventory OK")

    # ── 3) Top IPs suspectes ───────────────────────────────────────────────
    print("[GOLD] Calcul top threat IPs...")
    threat_ips = silver \
        .filter(col("is_anomaly") == 1) \
        .groupBy("srcip") \
        .agg(
            count("*").alias("nb_anomalies"),
            spark_max("severity").alias("max_severity"),
            count("event_id").alias("total_events"),
            spark_max("event_time").alias("last_alert"),
        ).filter(col("srcip").isNotNull() & (trim(col("srcip")) != "")) \
        .orderBy(col("nb_anomalies").desc())

    threat_ips.coalesce(1).writeTo("iceberg.gold.fg_top_threat_ips") \
        .using("iceberg") \
        .option("write_compression", "zstd") \
        .overwritePartitions()
    print("[GOLD] fg_top_threat_ips OK")

    # ── 4) SD-WAN performance ────────────────────────────────────────────
    print("[GOLD] Calcul SD-WAN performance...")
    sdwan = silver \
        .filter(col("latency") > 0) \
        .groupBy("vwlid", window(col("event_time"), "1 hour")) \
        .agg(
            avg("latency").alias("avg_latency_ms"),
            spark_max("latency").alias("max_latency_ms"),
            percentile_approx("latency", 0.95).alias("p95_latency_ms"),
            count(when(col("latency") > 20, 1)).alias("degraded_sessions"),
            count(when(col("latency") > 50, 1)).alias("critical_sessions"),
            count("*").alias("total_sessions"),
            spark_sum("total_bytes").alias("total_bytes"),
        ).select(
            col("window.start").alias("window_start"),
            "vwlid", "avg_latency_ms", "max_latency_ms",
            "p95_latency_ms", "degraded_sessions",
            "critical_sessions", "total_sessions", "total_bytes"
        )

    sdwan.coalesce(2).writeTo("iceberg.gold.fg_sdwan_performance") \
        .using("iceberg") \
        .option("write_compression", "zstd") \
        .overwritePartitions()
    print("[GOLD] fg_sdwan_performance OK")

    # ── 5) Trafic par politique ──────────────────────────────────────────
    print("[GOLD] Calcul policy traffic...")
    policy = silver \
        .filter(trim(col("policyname")) != "") \
        .groupBy("policyname", "policytype") \
        .agg(
            count("*").alias("total_sessions"),
            count(when(col("is_anomaly") == 1, 1)).alias("blocked_sessions"),
            spark_sum("total_bytes").alias("total_bytes"),
            avg("latency").alias("avg_latency_ms"),
            countDistinct("srcip").alias("unique_src_ips"),
            countDistinct("dstip").alias("unique_dst_ips"),
        )

    policy.coalesce(2).writeTo("iceberg.gold.fg_policy_traffic") \
        .using("iceberg") \
        .option("write_compression", "zstd") \
        .overwritePartitions()
    print("[GOLD] fg_policy_traffic OK")

    # ── 6) Distribution protocoles par jour ────────────────────────────────
    print("[GOLD] Calcul protocol stats...")
    proto_stats = silver \
        .groupBy("tag", window(col("event_time"), "1 day")) \
        .agg(
            count("*").alias("total_sessions"),
            count(when(col("is_anomaly") == 1, 1)).alias("anomaly_sessions"),
            spark_sum("total_bytes").alias("total_bytes"),
            avg("latency").alias("avg_latency_ms"),
            countDistinct("srcip").alias("unique_sources"),
        ).select(
            col("window.start").alias("day"),
            "tag", "total_sessions", "anomaly_sessions",
            "total_bytes", "avg_latency_ms", "unique_sources"
        )

    proto_stats.coalesce(2).writeTo("iceberg.gold.fg_protocol_stats") \
        .using("iceberg") \
        .option("write_compression", "zstd") \
        .overwritePartitions()
    print("[GOLD] fg_protocol_stats OK")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Optimisations Iceberg + AQE (déjà en partie dans build_spark)
    spark.conf.set("spark.sql.iceberg.merge-on-read.enabled", "true")

    ensure_tables(spark)

    # Exécution séquentielle avec persistance sur disque entre étapes
    write_bronze(spark)
    write_silver(spark)
    write_gold(spark)

    print("\n" + "=" * 60)
    print("[BATCH] Pipeline FortiGate termine avec succes!")
    print("  Bronze → iceberg.bronze.fg_events_raw")
    print("  Silver → iceberg.silver.fg_events_clean")
    print("  Gold   → iceberg.gold.fg_kpi_hourly")
    print("  Gold   → iceberg.gold.fg_device_inventory")
    print("  Gold   → iceberg.gold.fg_top_threat_ips")
    print("  Gold   → iceberg.gold.fg_sdwan_performance")
    print("  Gold   → iceberg.gold.fg_policy_traffic")
    print("  Gold   → iceberg.gold.fg_protocol_stats")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
