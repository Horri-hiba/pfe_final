#!/usr/bin/env python3
"""
Initialise MinIO: crée les buckets séparés pour chaque couche
  - iceberg         → catalogue Iceberg REST (metadata)
  - iceberg-bronze  → données brutes (Bronze)
  - iceberg-silver  → données nettoyées (Silver)
  - iceberg-gold    → KPIs agrégés (Gold)
  - parquet-store   → fichiers Parquet bruts
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
    "iceberg",
    "iceberg-bronze",
    "iceberg-silver",
    "iceberg-gold",
    "parquet-store",
]

existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]

for bucket in BUCKETS:
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        print(f"[MinIO] Bucket '{bucket}' cree")
    else:
        print(f"[MinIO] Bucket '{bucket}' existe deja")

print("[MinIO] Initialisation terminee")