"""
Spark Batch Pipeline — FortiGate Network Logs
Aligné sur le schéma real-time (Vector → ClickHouse)

Kafka topics batch :
  network_security, network_traffic, network_perf, network_devices, network_all

→ Iceberg Bronze / Silver / Gold  (MinIO S3 via REST catalog)

Schéma identique au real-time :
  srcip, dstip, action, status, severity_label, is_anomaly,
  latency, sentbyte, rcvdbyte, total_bytes, vwlid, vwlquality,
  devtype, srchwvendor, srcmac, policyname, service, sessionid ...
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

# ── Config ────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = "http://minio1:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
CATALOG_URI      = "http://iceberg-rest:8181"
WAREHOUSE        = "s3://iceberg/"
KAFKA_SERVERS    = "kafka-batch:9092"
TOPICS           = "fg_security_events,fg_traffic_flow,fg_performance,fg_device_inventory,fg_raw_all"

# ── Schéma JSON aligné sur Vector/real-time ───────────────────────────────────
# Tous les champs FortiGate enrichis par Logstash (même logique que Vector)
MSG_SCHEMA = StructType([
    # Identifiants
    StructField("event_id",      StringType()),
    StructField("device_id",     StringType()),
    # Timestamps
    StructField("ts",            StringType()),   # timestamp FortiGate original
    StructField("eventtime",     StringType()),   # eventtime= du msg kv
    StructField("ingested_at",   StringType()),
    # Message
    StructField("message",       StringType()),
    StructField("tag",           StringType()),
    StructField("program",       StringType()),
    StructField("seq",           StringType()),
    StructField("facility",      IntegerType()),
    # Catégorisation FortiGate
    StructField("event_type",    StringType()),
    StructField("subtype",       StringType()),
    StructField("action",        StringType()),
    StructField("status",        StringType()),
    # Réseau
    StructField("srcip",         StringType()),
    StructField("dstip",         StringType()),
    StructField("srcport",       IntegerType()),
    StructField("dstport",       IntegerType()),
    StructField("proto",         IntegerType()),
    StructField("service",       StringType()),
    # Politique
    StructField("policyname",    StringType()),
    StructField("policytype",    StringType()),
    StructField("poluuid",       StringType()),
    # Métriques
    StructField("sentbyte",      LongType()),
    StructField("rcvdbyte",      LongType()),
    StructField("total_bytes",   LongType()),
    StructField("latency",       FloatType()),
    StructField("sessionid",     StringType()),
    # SD-WAN
    StructField("vwlid",         IntegerType()),
    StructField("vwlquality",    StringType()),
    # Inventaire
    StructField("devname",       StringType()),
    StructField("devtype",       StringType()),
    StructField("srchwvendor",   StringType()),
    StructField("srcmac",        StringType()),
    # Sévérité / anomalie
    StructField("severity",      IntegerType()),
    StructField("severity_label", StringType()),
    StructField("is_anomaly",    IntegerType()),
])


def build_spark():
    return (
        SparkSession.builder
        .appName("FortiGateBatchPipeline-Iceberg")
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
        .config("spark.sql.iceberg.compression-codec", "uncompressed")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "512m")
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
    """
    Crée les namespaces et tables Iceberg.
    Schéma aligné sur le real-time ClickHouse (même champs FortiGate).
    """
    for ns in ["bronze", "silver", "gold"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{ns}")

    # ── BRONZE — données brutes FortiGate (tous champs) ───────────────────────
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
    """)

    # ── SILVER — données nettoyées et typées ──────────────────────────────────
    # Miroir du schéma ClickHouse t_raw_events (real-time)
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
    """)

    # ── GOLD — KPIs horaires (miroir des vues Grafana real-time) ─────────────
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
    """)

    # ── GOLD — Inventaire devices (miroir t_devices real-time) ───────────────
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
    """)

    # ── GOLD — Top IPs suspectes (miroir v_top_threat_ips real-time) ─────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.fg_top_threat_ips (
            srcip           STRING,
            nb_anomalies    BIGINT,
            max_severity    INT,
            total_events    BIGINT,
            last_alert      TIMESTAMP
        ) USING iceberg
    """)

    # ── GOLD — Performance SD-WAN par lien ────────────────────────────────────
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
    """)

    # ── GOLD — Trafic par politique FortiGate ─────────────────────────────────
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
    """)

    # ── GOLD — Distribution des protocoles applicatifs par jour ──────────────
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
    """)

    print("[TABLES] Iceberg OK — bronze/silver/gold alignés sur schéma real-time")


def write_bronze(spark, raw_kafka):
    """Bronze : données brutes FortiGate depuis Kafka — tous champs préservés."""
    df = raw_kafka.select(
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
        col("data.device_id").isNotNull() & (trim(col("data.device_id")) != "")
    )

    # Dédupliquer : un même event_id peut arriver dans plusieurs topics
    df_dedup = df.dropDuplicates(["event_id"])

    df_dedup.write.format("iceberg").mode("append").saveAsTable(
        "iceberg.bronze.fg_events_raw"
    )
    cnt = df_dedup.count()
    print(f"[BRONZE] {cnt} lignes → iceberg.bronze.fg_events_raw")
    return df_dedup


def write_silver(spark, bronze_df):
    """
    Silver : nettoyage et typage.
    Même transformation que ClickHouse mv_* (real-time).
    """
    df = bronze_df \
        .withColumn(
            "event_time",
            coalesce(
                to_timestamp(trim(col("ts")), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                to_timestamp(trim(col("ts")), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(trim(col("ts")), "yyyy-MM-dd'T'HH:mm:ss"),
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
            col("srcport"),
            col("dstport"),
            col("proto"),
            trim(col("service")).alias("service"),
            trim(col("policyname")).alias("policyname"),
            trim(col("policytype")).alias("policytype"),
            trim(col("poluuid")).alias("poluuid"),
            coalesce(col("sentbyte"),   lit(0)).alias("sentbyte"),
            coalesce(col("rcvdbyte"),   lit(0)).alias("rcvdbyte"),
            coalesce(col("total_bytes"),lit(0)).alias("total_bytes"),
            coalesce(col("latency"),    lit(0.0)).alias("latency"),
            trim(col("sessionid")).alias("sessionid"),
            coalesce(col("vwlid"),      lit(0)).alias("vwlid"),
            trim(col("vwlquality")).alias("vwlquality"),
            trim(col("devname")).alias("devname"),
            trim(col("devtype")).alias("devtype"),
            trim(col("srchwvendor")).alias("srchwvendor"),
            trim(col("srcmac")).alias("srcmac"),
            coalesce(col("severity"),   lit(1)).alias("severity"),
            trim(col("severity_label")).alias("severity_label"),
            coalesce(col("is_anomaly"), lit(0)).alias("is_anomaly"),
            col("hour_of_day"),
            col("day_of_week"),
            col("proto_label"),
            col("latency_bucket"),
            col("is_vpn_failure"),
        ).filter(col("event_time").isNotNull())

    # coalesce(4) : limite les writers Parquet parallèles pour économiser la mémoire
    df_write = df.coalesce(4) 
    # Compter AVANT le write pour éviter un double scan du DataFrame
    cnt = df_write.count()
    df_write.write.format("iceberg").mode("append").saveAsTable(
        "iceberg.silver.fg_events_clean"
    )
    print(f"[SILVER] {cnt} lignes → iceberg.silver.fg_events_clean")
    return df_write


def write_gold(spark, silver_df):
    """
    Gold : KPIs et agrégats.
    Miroir des vues analytiques Grafana real-time :
      v_severity_per_hour, v_top_threat_ips, v_device_summary, v_high_latency
    """
    silver_cached = silver_df.repartition(8).cache()
    silver_cached.count()

    # ── KPI horaires (miroir v_severity_per_hour + stats perf) ───────────────
    kpi = silver_cached.groupBy(
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
    kpi.coalesce(4).write.format("iceberg").mode("append").saveAsTable(
        "iceberg.gold.fg_kpi_hourly"
    )
    print(f"[GOLD] KPI horaires → iceberg.gold.fg_kpi_hourly")

    # ── Inventaire devices (miroir v_device_summary real-time) ───────────────
    devices = silver_cached.groupBy(
        "device_id", "devname", "devtype", "srchwvendor", "srcmac"
    ).agg(
        count("*").alias("total_events"),
        count(when(col("is_anomaly") == 1, 1)).alias("total_anomalies"),
        avg("latency").alias("avg_latency_ms"),
        spark_max("latency").alias("max_latency_ms"),
        spark_sum("total_bytes").alias("total_bytes"),
        spark_max("event_time").alias("last_seen"),
    ).filter(col("device_id").isNotNull())

    devices.coalesce(4).write.format("iceberg").mode("append").saveAsTable(
        "iceberg.gold.fg_device_inventory"
    )
    print(f"[GOLD] Inventaire devices → iceberg.gold.fg_device_inventory")

    # ── Top IPs suspectes (miroir v_top_threat_ips real-time) ─────────────────
    threat_ips = silver_cached \
        .filter(col("is_anomaly") == 1) \
        .groupBy("srcip") \
        .agg(
            count("*").alias("nb_anomalies"),
            spark_max("severity").alias("max_severity"),
            count("event_id").alias("total_events"),
            spark_max("event_time").alias("last_alert"),
        ).filter(col("srcip").isNotNull() & (trim(col("srcip")) != "")) \
        .orderBy(col("nb_anomalies").desc())

    threat_ips.coalesce(2).write.format("iceberg").mode("append").saveAsTable(
        "iceberg.gold.fg_top_threat_ips"
    )
    print(f"[GOLD] Top IPs suspectes → iceberg.gold.fg_top_threat_ips")

    # ── SD-WAN performance par lien (vwlid) ───────────────────────────────────
    sdwan = silver_cached \
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
    sdwan.coalesce(4).write.format("iceberg").mode("append").saveAsTable(
        "iceberg.gold.fg_sdwan_performance"
    )
    print(f"[GOLD] SD-WAN performance → iceberg.gold.fg_sdwan_performance")

    # ── Trafic par politique FortiGate ────────────────────────────────────────
    policy = silver_cached \
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
    policy.coalesce(4).write.format("iceberg").mode("append").saveAsTable(
        "iceberg.gold.fg_policy_traffic"
    )
    print(f"[GOLD] Trafic par politique → iceberg.gold.fg_policy_traffic")

    # ── Distribution protocoles applicatifs par jour ──────────────────────────
    proto_stats = silver_cached \
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
    proto_stats.coalesce(4).write.format("iceberg").mode("append").saveAsTable(
        "iceberg.gold.fg_protocol_stats"
    )
    print(f"[GOLD] Stats protocoles → iceberg.gold.fg_protocol_stats")

    silver_cached.unpersist()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)

    print(f"[BATCH] Lecture Kafka topics : {TOPICS}")
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

    # Parser le JSON de chaque message Kafka
    parsed = raw_kafka.withColumn(
        "data", from_json(col("value").cast("string"), MSG_SCHEMA)
    )

    total = raw_kafka.count()
    print(f"[BATCH] {total} messages lus depuis Kafka")

    b = write_bronze(spark, parsed)
    s = write_silver(spark, b)
    write_gold(spark, s)

    print("\n" + "=" * 60)
    print("[BATCH] Pipeline FortiGate terminé avec succès!")
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