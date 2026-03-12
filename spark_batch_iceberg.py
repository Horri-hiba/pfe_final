"""
Spark Batch Pipeline — Network Event Logs
Kafka: network_all, network_alerts, network_metrics, network_info
→ Iceberg Bronze / Silver / Gold  (tout dans bucket 'iceberg', séparé par namespace)
→ Parquet brut (parquet-store)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    when, trim, count, avg, max as spark_max,
    window, hour, dayofweek
)
from pyspark.sql.types import StructType, StructField, StringType

MINIO_ENDPOINT   = "http://minio1:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
CATALOG_URI      = "http://iceberg-rest:8181"
WAREHOUSE        = "s3://iceberg/"
KAFKA_SERVERS    = "kafka-batch:9092"
TOPICS           = "network_all,network_alerts,network_metrics,network_info"

MSG_SCHEMA = StructType([
    StructField("event_id",    StringType()),
    StructField("device_id",   StringType()),
    StructField("event_time",  StringType()),
    StructField("message",     StringType()),
    StructField("event_type",  StringType()),
    StructField("ip_address",  StringType()),
    StructField("severity",    StringType()),
    StructField("alert_level", StringType()),
    StructField("ingested_at", StringType()),
])


def build_spark():
    return (
        SparkSession.builder
        .appName("NetworkBatchPipeline-Iceberg")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", CATALOG_URI)
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
        .config("spark.sql.iceberg.compression-codec", "uncompressed")
        .config("spark.sql.catalog.iceberg.write.parquet.compression-codec", "uncompressed")
        .config("spark.sql.iceberg.parquet.dict-size-bytes", "1")
        .config("spark.sql.iceberg.parquet.page-size-bytes", "524288")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def ensure_tables(spark):
    """Crée les namespaces et tables Iceberg — sans LOCATION custom."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    # BRONZE — données brutes
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.network_events_raw (
            ingested_at   TIMESTAMP,
            kafka_topic   STRING,
            kafka_offset  BIGINT,
            event_id      STRING,
            device_id     STRING,
            event_time    STRING,
            message       STRING,
            event_type    STRING,
            ip_address    STRING,
            severity      STRING,
            alert_level   STRING
        ) USING iceberg
        PARTITIONED BY (days(ingested_at))
    """)

    # SILVER — données nettoyées
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.network_events_clean (
            ingested_at   TIMESTAMP,
            event_time    TIMESTAMP,
            kafka_topic   STRING,
            event_id      BIGINT,
            device_id     INT,
            message       STRING,
            event_type    STRING,
            ip_address    STRING,
            severity      INT,
            alert_level   STRING,
            is_alert      BOOLEAN,
            hour_of_day   INT,
            day_of_week   INT
        ) USING iceberg
        PARTITIONED BY (days(event_time), alert_level)
    """)

    # GOLD — KPIs horaires
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.network_kpi_hourly (
            window_start    TIMESTAMP,
            window_end      TIMESTAMP,
            event_type      STRING,
            alert_level     STRING,
            total_events    BIGINT,
            critical_count  BIGINT,
            major_count     BIGINT,
            minor_count     BIGINT,
            warning_count   BIGINT,
            info_count      BIGINT,
            avg_severity    DOUBLE,
            unique_devices  BIGINT
        ) USING iceberg

    """)

    # GOLD — Top devices
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.top_devices_alerts (
            device_id      INT,
            event_type     STRING,
            total_events   BIGINT,
            critical_count BIGINT,
            major_count    BIGINT,
            avg_severity   DOUBLE,
            last_seen      TIMESTAMP
        ) USING iceberg
    """)

    print("[TABLES] Iceberg OK — bronze / silver / gold dans s3://iceberg/")


def write_bronze(spark, raw_kafka):
    df = raw_kafka.select(
        current_timestamp().alias("ingested_at"),
        col("topic").alias("kafka_topic"),
        col("offset").cast("long").alias("kafka_offset"),
        col("data.event_id"), col("data.device_id"), col("data.event_time"),
        col("data.message").alias("message"),
        col("data.event_type"), col("data.ip_address"),
        col("data.severity"), col("data.alert_level"),
    ).filter(col("data.event_id").isNotNull() & (trim(col("data.event_id")) != ""))

    df.write.format("iceberg").mode("append").saveAsTable("iceberg.bronze.network_events_raw")
    cnt = df.count()
    print(f"[BRONZE] {cnt} lignes ecrites dans iceberg.bronze.network_events_raw")
    return df


def write_silver(spark, bronze_df):
    df = bronze_df \
        .withColumn("event_time_ts", to_timestamp(trim(col("event_time")), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("severity_int", col("severity").cast("int")) \
        .withColumn("is_alert", col("severity_int") >= 4) \
        .withColumn("hour_of_day", hour(col("event_time_ts"))) \
        .withColumn("day_of_week", dayofweek(col("event_time_ts"))) \
        .select(
            col("ingested_at"),
            col("event_time_ts").alias("event_time"),
            col("kafka_topic"),
            col("event_id").cast("long"),
            col("device_id").cast("int"),
            trim(col("message")).alias("message"),
            trim(col("event_type")).alias("event_type"),
            trim(col("ip_address")).alias("ip_address"),
            col("severity_int").alias("severity"),
            trim(col("alert_level")).alias("alert_level"),
            col("is_alert"),
            col("hour_of_day"),
            col("day_of_week"),
        ).filter(col("event_time").isNotNull())

    df.write.format("iceberg").mode("append").saveAsTable("iceberg.silver.network_events_clean")
    cnt = df.count()
    print(f"[SILVER] {cnt} lignes ecrites dans iceberg.silver.network_events_clean")
    return df


def write_gold(spark, silver_df):
    # Cache silver avec plus de partitions pour distribuer la charge
    silver_cached = silver_df.repartition(8).cache()
    silver_cached.count()  # force materialisation du cache

    # ── KPI horaires ──────────────────────────────────────────────────────────
    kpi = silver_cached.groupBy(
        window(col("event_time"), "1 hour"),
        col("event_type"),
        col("alert_level")
    ).agg(
        count("*").alias("total_events"),
        count(when(col("severity") == 5, 1)).alias("critical_count"),
        count(when(col("severity") == 4, 1)).alias("major_count"),
        count(when(col("severity") == 3, 1)).alias("minor_count"),
        count(when(col("severity") == 2, 1)).alias("warning_count"),
        count(when(col("severity") == 1, 1)).alias("info_count"),
        avg("severity").alias("avg_severity"),
        count("device_id").alias("unique_devices"),
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_type", "alert_level", "total_events",
        "critical_count", "major_count", "minor_count",
        "warning_count", "info_count", "avg_severity", "unique_devices"
    )

    # coalesce(4) : répartir l'écriture sur 4 fichiers, évite un seul task géant
    kpi_to_write = kpi.coalesce(4)
    kpi_to_write.write.format("iceberg").mode("append").saveAsTable("iceberg.gold.network_kpi_hourly")
    kpi_count = spark.sql("SELECT COUNT(*) FROM iceberg.gold.network_kpi_hourly").collect()[0][0]
    print(f"[GOLD] {kpi_count} KPI horaires dans iceberg.gold.network_kpi_hourly")

    # ── Top devices ───────────────────────────────────────────────────────────
    top = silver_cached.groupBy("device_id", "event_type").agg(
        count("*").alias("total_events"),
        count(when(col("severity") == 5, 1)).alias("critical_count"),
        count(when(col("severity") == 4, 1)).alias("major_count"),
        avg("severity").alias("avg_severity"),
        spark_max("event_time").alias("last_seen"),
    ).filter(col("device_id").isNotNull())

    # coalesce(4) : même logique
    top_to_write = top.coalesce(4)
    top_to_write.write.format("iceberg").mode("append").saveAsTable("iceberg.gold.top_devices_alerts")
    top_count = spark.sql("SELECT COUNT(*) FROM iceberg.gold.top_devices_alerts").collect()[0][0]
    print(f"[GOLD] {top_count} device stats dans iceberg.gold.top_devices_alerts")

    silver_cached.unpersist()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)

    print(f"[BATCH] Lecture Kafka: {TOPICS}")
    raw_kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    parsed = raw_kafka.withColumn(
        "data", from_json(col("value").cast("string"), MSG_SCHEMA)
    )
    print(f"[BATCH] {raw_kafka.count()} messages lus depuis Kafka")

    b = write_bronze(spark, parsed)
    s = write_silver(spark, b)
    write_gold(spark, s)

    print("\n========================================")
    print("[BATCH] Pipeline termine avec succes!")
    print("  Bronze → iceberg.bronze.network_events_raw")
    print("  Silver → iceberg.silver.network_events_clean")
    print("  Gold   → iceberg.gold.network_kpi_hourly")
    print("  Gold   → iceberg.gold.top_devices_alerts")
    print("========================================")
    spark.stop()


if __name__ == "__main__":
    main()