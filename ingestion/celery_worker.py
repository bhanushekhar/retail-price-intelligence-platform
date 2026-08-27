"""
Celery worker: consumes scraped batches off the Redis-backed queue and
does the "heavy" work (writing the gzip JSONL file + uploading to MinIO)
away from the scraper process.

Start the worker (separate terminal, keep it running):
    celery -A ingestion.celery_worker worker --loglevel=info --pool=solo

(--pool=solo is required on Windows.)
"""

import json
import gzip
import os
import uuid
from pathlib import Path

from celery import Celery

from ingestion.upload_to_lake import upload_file

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")
BROKER_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

app = Celery("retailpulse", broker=BROKER_URL, backend=BROKER_URL)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)


@app.task(name="ingestion.process_scraped_batch", bind=True, max_retries=3)
def process_scraped_batch(self, records: list[dict], source_site: str, output_dir: str = "bronze_local"):
    """
    Takes a list of already-scraped records, writes them as a gzip JSONL
    file locally, then uploads that file to MinIO under bronze/<source_site>/.
    """
    try:
        batch_id = f"{source_site}_{uuid.uuid4().hex[:8]}"
        out_path = Path(output_dir) / source_site
        out_path.mkdir(parents=True, exist_ok=True)

        out_file = out_path / f"{batch_id}.json.gz"
        with gzip.open(out_file, "wt") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")

        object_key = f"bronze/{source_site}/{out_file.name}"
        uri = upload_file(str(out_file), object_key)

        print(f"[worker] wrote {len(records)} records -> {out_file}")
        print(f"[worker] uploaded -> {uri}")

        return {"local_file": str(out_file), "s3_uri": uri, "record_count": len(records)}

    except Exception as exc:
        print(f"[worker] FAILED, retrying... {exc}")
        raise self.retry(exc=exc, countdown=5)