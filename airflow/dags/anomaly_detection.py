"""
Anomaly Detection + GenAI Narrative Airflow DAG.

Two tasks:
  1. detect_anomalies       -> scans silver.price_history for price/stock
                                transitions crossing a threshold, inserts
                                rows into gold.anomalies (narrative = NULL)
  2. generate_narratives    -> for any anomaly rows still missing a
                                narrative, calls the LLM layer (Ollama,
                                with a template fallback) and fills it in

Run this AFTER bronze_to_silver and silver_to_gold, since it reads from
silver.price_history directly.

Trigger manually from the Airflow UI.
"""

import os
from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

from genai.anomaly_detector import detect_and_insert_anomalies
from genai.llm_narrative import generate_narrative

PG_CONN = dict(
    host=os.environ.get("POSTGRES_HOST", "postgres_app"),
    port=os.environ.get("POSTGRES_PORT", "5432"),
    dbname=os.environ.get("POSTGRES_DB", "retailpulse"),
    user=os.environ.get("POSTGRES_USER", "retailpulse"),
    password=os.environ.get("POSTGRES_PASSWORD", "retailpulse"),
)


def detect_anomalies(**context):
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = False
    cur = conn.cursor()

    stats = detect_and_insert_anomalies(cur)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Detection stats: {stats}")
    return stats


def generate_narratives(**context):
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute(
        """
        SELECT a.anomaly_id, p.title, a.anomaly_type, a.magnitude_pct,
               fh.price AS from_price, th.price AS to_price
        FROM gold.anomalies a
        JOIN silver.products p ON p.product_id = a.product_id
        LEFT JOIN silver.price_history fh ON fh.price_history_id = a.from_price_history_id
        JOIN silver.price_history th ON th.price_history_id = a.to_price_history_id
        WHERE a.narrative IS NULL
        """
    )
    rows = cur.fetchall()
    print(f"Found {len(rows)} anomalies needing a narrative")

    for anomaly_id, title, anomaly_type, magnitude_pct, from_price, to_price in rows:
        narrative = generate_narrative(title, anomaly_type, magnitude_pct, from_price, to_price)
        cur.execute(
            "UPDATE gold.anomalies SET narrative = %s WHERE anomaly_id = %s",
            (narrative, anomaly_id),
        )
        print(f"  [{anomaly_id}] {narrative}")

    conn.commit()
    cur.close()
    conn.close()

    return {"narratives_generated": len(rows)}


default_args = {"owner": "bhanu", "retries": 1}

with DAG(
    dag_id="anomaly_detection_and_narrative",
    default_args=default_args,
    description="Detect price/stock anomalies and generate plain-English LLM narratives",
    schedule=None,  # manual trigger only, for now
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retailpulse", "genai", "aiops"],
) as dag:

    detect_task = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
    )

    narrative_task = PythonOperator(
        task_id="generate_narratives",
        python_callable=generate_narratives,
    )

    detect_task >> narrative_task