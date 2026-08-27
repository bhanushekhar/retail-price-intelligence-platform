"""
Bronze -> Silver Airflow DAG.

Reads new gzip JSONL files from MinIO (bronze/books_toscrape/), and for
each product record, applies SCD Type-2 logic against Postgres:

  - New product?              -> insert into silver.products, then insert
                                  the first price_history row (is_current=true)
  - Price/stock unchanged?    -> do nothing (no duplicate rows)
  - Price/stock changed?      -> close the old price_history row
                                  (valid_to=now(), is_current=false) and
                                  insert a brand-new current row

Trigger manually from the Airflow UI (Play button) — this DAG has no
automatic schedule yet, on purpose, so you can run it on demand while
building and testing.
"""

import os
import io
import json
import gzip
from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion.upload_to_lake import list_objects, download_bytes

SOURCE_SITE = "books_toscrape"
BRONZE_PREFIX = f"bronze/{SOURCE_SITE}/"

PG_CONN = dict(
    host=os.environ.get("POSTGRES_HOST", "postgres_app"),
    port=os.environ.get("POSTGRES_PORT", "5432"),
    dbname=os.environ.get("POSTGRES_DB", "retailpulse"),
    user=os.environ.get("POSTGRES_USER", "retailpulse"),
    password=os.environ.get("POSTGRES_PASSWORD", "retailpulse"),
)


def get_or_create_product(cur, source_site: str, source_url: str, title: str, category: str = None) -> int:
    """Look up a product by its source URL, creating it if this is the first time we've seen it."""
    cur.execute("SELECT product_id FROM silver.products WHERE source_url = %s", (source_url,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """INSERT INTO silver.products (source_site, source_url, title, category)
           VALUES (%s, %s, %s, %s) RETURNING product_id""",
        (source_site, source_url, title, category),
    )
    return cur.fetchone()[0]


def apply_scd2(cur, product_id: int, price: float, currency: str, in_stock: bool, scrape_batch_id: str) -> str:
    """
    Core SCD Type-2 logic. Returns one of: 'inserted_first', 'changed', 'unchanged'.
    """
    cur.execute(
        """SELECT price_history_id, price, in_stock FROM silver.price_history
           WHERE product_id = %s AND is_current = true""",
        (product_id,),
    )
    current = cur.fetchone()

    if current is None:
        cur.execute(
            """INSERT INTO silver.price_history
               (product_id, price, currency, in_stock, valid_from, is_current, scrape_batch_id)
               VALUES (%s, %s, %s, %s, now(), true, %s)""",
            (product_id, price, currency, in_stock, scrape_batch_id),
        )
        return "inserted_first"

    _, current_price, current_in_stock = current
    changed = (float(current_price) != float(price)) or (current_in_stock != in_stock)

    if not changed:
        return "unchanged"

    # Close out the old "current" row before inserting the new one —
    # this is the heart of SCD Type-2: never overwrite, always append.
    cur.execute(
        """UPDATE silver.price_history
           SET valid_to = now(), is_current = false
           WHERE product_id = %s AND is_current = true""",
        (product_id,),
    )
    cur.execute(
        """INSERT INTO silver.price_history
           (product_id, price, currency, in_stock, valid_from, is_current, scrape_batch_id)
           VALUES (%s, %s, %s, %s, now(), true, %s)""",
        (product_id, price, currency, in_stock, scrape_batch_id),
    )
    return "changed"


def load_bronze_to_silver(**context):
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SELECT object_key FROM silver.processed_files")
    already_processed = {r[0] for r in cur.fetchall()}

    all_keys = list_objects(BRONZE_PREFIX)
    new_keys = [k for k in all_keys if k not in already_processed]

    print(f"Found {len(all_keys)} total Bronze objects, {len(new_keys)} are new")

    stats = {"inserted_first": 0, "changed": 0, "unchanged": 0, "files_processed": 0}

    for key in new_keys:
        raw_bytes = download_bytes(key)
        with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes)) as gz:
            lines = gz.read().decode("utf-8").splitlines()

        for line in lines:
            if not line.strip():
                continue
            rec = json.loads(line)

            product_id = get_or_create_product(
                cur, rec["source_site"], rec["source_url"], rec["title"]
            )
            result = apply_scd2(
                cur,
                product_id,
                rec["price"],
                rec.get("currency", "GBP"),
                rec["in_stock"],
                rec.get("scrape_batch_id", key),
            )
            stats[result] += 1

        cur.execute(
            "INSERT INTO silver.processed_files (object_key) VALUES (%s) ON CONFLICT DO NOTHING",
            (key,),
        )
        stats["files_processed"] += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Done. Stats: {stats}")
    return stats


default_args = {"owner": "bhanu", "retries": 1}

with DAG(
    dag_id="bronze_to_silver_books_toscrape",
    default_args=default_args,
    description="Load new Bronze JSONL files from MinIO into Silver (SCD Type-2) Postgres tables",
    schedule=None,  # manual trigger only, for now — automate later once this is proven out
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retailpulse", "bronze-to-silver"],
) as dag:

    load_task = PythonOperator(
        task_id="load_bronze_to_silver",
        python_callable=load_bronze_to_silver,
    )