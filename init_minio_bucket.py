#!/usr/bin/env python3
"""
Initialise MinIO : crée les buckets nécessaires au pipeline batch Iceberg.

NOTE : Dans cette version, toutes les tables Iceberg résident dans le bucket
"iceberg" (warehouse=s3://iceberg/). Les buckets "iceberg-bronze",
"iceberg-silver", "iceberg-gold" sont créés pour une future évolution
(multi-warehouse) mais ne sont pas utilisés par Spark actuellement.
"""
import boto3
from botocore.client import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio1:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

BUCKETS = [
    "iceberg",          # warehouse principal (metadata + data tables)
    "iceberg-bronze",   # réservé future évolution
    "iceberg-silver",   # réservé future évolution
    "iceberg-gold",     # réservé future évolution
    "parquet-store",    # stockage Parquet brut si besoin
]

existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]

for bucket in BUCKETS:
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        print(f"[MinIO] Bucket '{bucket}' cree")
    else:
        print(f"[MinIO] Bucket '{bucket}' existe deja")

print("[MinIO] Initialisation terminee")
