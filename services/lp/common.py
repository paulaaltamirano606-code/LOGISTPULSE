"""Utilidades compartidas: conexión a Postgres (ODS/DW) y a MinIO (Data Lake)."""
import os
import time

import boto3
import pandas as pd
import psycopg2
from botocore.client import Config

RAW = "lake-raw"          # zona cruda del Data Lake (tal como llega la fuente)
CURATED = "lake-curated"  # zona curada (parquet, modelos, scores)


def pg(retries: int = 30):
    last = None
    for _ in range(retries):
        try:
            return psycopg2.connect(
                host=os.getenv("PGHOST", "postgres"),
                port=os.getenv("PGPORT", "5432"),
                dbname=os.getenv("PGDATABASE", "logistpulse"),
                user=os.getenv("PGUSER", "lp"),
                password=os.getenv("PGPASSWORD", "lp_secret"),
            )
        except psycopg2.OperationalError as exc:  # Postgres aún arrancando
            last = exc
            time.sleep(2)
    raise last


def query_df(sql: str, params=None) -> pd.DataFrame:
    with pg() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minioadmin123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def put_bytes(bucket: str, key: str, data: bytes) -> None:
    s3().put_object(Bucket=bucket, Key=key, Body=data)


def get_bytes(bucket: str, key: str) -> bytes:
    return s3().get_object(Bucket=bucket, Key=key)["Body"].read()
