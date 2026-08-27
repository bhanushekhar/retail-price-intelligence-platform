"""
Small helper for uploading Bronze-layer files to MinIO (S3-compatible).

MinIO speaks the same API as real AWS S3, so this same code would work
against actual S3 later just by changing the endpoint_url and credentials —
that's the whole point of using an S3-compatible store locally.
"""

import os
import boto3
from botocore.client import Config

# Defaults match .env.example / docker-compose.yml. Override via env vars
# if you ever change the MinIO credentials.
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "retailpulse")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "retailpulse123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "retailpulse-lake")


def get_minio_client():
    """Create a boto3 S3 client pointed at the local MinIO instance."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",  # MinIO ignores this, but boto3 requires *something*
    )


def upload_file(local_path: str, object_key: str, bucket: str = MINIO_BUCKET) -> str:
    """
    Upload a local file to MinIO under the given object key (the "path"
    inside the bucket). Returns the full s3:// style URI for logging.

    Example object_key: "bronze/books_toscrape/2026-08-08/batch_ab12cd34.jsonl"
    """
    client = get_minio_client()
    client.upload_file(local_path, bucket, object_key)
    return f"s3://{bucket}/{object_key}"


def list_objects(prefix: str, bucket: str = MINIO_BUCKET) -> list[str]:
    """
    List all object keys in the bucket under the given prefix.
    Used by the Airflow DAG to discover new Bronze files to process.
    """
    client = get_minio_client()
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def download_bytes(object_key: str, bucket: str = MINIO_BUCKET) -> bytes:
    """
    Download an object's raw bytes from MinIO (without saving to disk).
    Used by the Airflow DAG to pull Bronze files directly into memory.
    """
    client = get_minio_client()
    response = client.get_object(Bucket=bucket, Key=object_key)
    return response["Body"].read()

    

if __name__ == "__main__":
    # Quick manual test: uploads this very file so you can confirm the
    # bucket/credentials work before wiring it into the real scraper.
    uri = upload_file(__file__, "test/upload_check.py")
    print(f"Uploaded test file to {uri}")
    print("Check the MinIO console at http://localhost:9001 to confirm it landed.")

