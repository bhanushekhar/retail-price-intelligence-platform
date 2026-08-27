"""
RetailPulse API — REST endpoints + a live WebSocket feed.

Run locally (from the retailpulse/ root, with your venv active):
    uvicorn api.main:app --reload --port 8000

Then open http://localhost:8000/docs for interactive Swagger UI, which
is genuinely useful for testing every endpoint by hand without writing
any client code.

This connects to Postgres over localhost:5432 — since it runs as a
normal process on your machine (not inside Docker), unlike the Airflow
containers which had to use the "postgres_app" service name instead.
"""

import os
import json
import asyncio
from typing import List

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

PG_CONN = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=os.environ.get("POSTGRES_PORT", "5432"),
    dbname=os.environ.get("POSTGRES_DB", "retailpulse"),
    user=os.environ.get("POSTGRES_USER", "retailpulse"),
    password=os.environ.get("POSTGRES_PASSWORD", "retailpulse"),
)


def get_conn():
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = True
    return conn


app = FastAPI(title="RetailPulse API")

# Wide-open CORS is fine for local dev; a real deployment would restrict this.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"service": "RetailPulse API", "status": "ok"}


@app.get("/products")
def list_products():
    """Current price + stock status for every tracked product."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT p.product_id, p.title, p.source_site, p.source_url,
               ph.price, ph.currency, ph.in_stock, ph.valid_from
        FROM silver.products p
        JOIN silver.price_history ph
            ON ph.product_id = p.product_id AND ph.is_current = true
        ORDER BY p.product_id
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@app.get("/products/{product_id}/history")
def product_history(product_id: int):
    """Full SCD2 price history for one product — every price change ever recorded."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT price_history_id, price, currency, in_stock, valid_from, valid_to, is_current
        FROM silver.price_history
        WHERE product_id = %s
        ORDER BY valid_from
        """,
        (product_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@app.get("/anomalies")
def list_anomalies():
    """All detected anomalies, most recent first, with their LLM-generated narratives."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT a.anomaly_id, a.product_id, p.title, a.anomaly_type,
               a.magnitude_pct, a.narrative, a.detected_at, a.resolved
        FROM gold.anomalies a
        JOIN silver.products p ON p.product_id = a.product_id
        ORDER BY a.detected_at DESC
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ============================================================
# WebSocket: live anomaly push
# ============================================================

connected_clients: List[WebSocket] = []


@app.websocket("/ws/prices")
async def ws_prices(websocket: WebSocket):
    """
    Clients connect here and just listen — the server pushes new anomaly
    events the moment the background poller finds one. No polling needed
    on the client side.
    """
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            # We don't expect messages from the client, but this keeps the
            # connection alive and lets us detect a clean disconnect.
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)


async def poll_for_new_anomalies():
    """
    Background task started at app startup. Every 3 seconds, checks for
    anomaly rows newer than the last one we've already broadcast, and
    pushes each one to every connected WebSocket client.
    """
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT COALESCE(MAX(anomaly_id), 0) AS max_id FROM gold.anomalies")
    last_seen_id = cur.fetchone()["max_id"]
    cur.close()
    conn.close()

    while True:
        await asyncio.sleep(3)
        if not connected_clients:
            continue

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT a.anomaly_id, a.product_id, p.title, a.anomaly_type,
                   a.magnitude_pct, a.narrative, a.detected_at
            FROM gold.anomalies a
            JOIN silver.products p ON p.product_id = a.product_id
            WHERE a.anomaly_id > %s
            ORDER BY a.anomaly_id
            """,
            (last_seen_id,),
        )
        new_rows = cur.fetchall()
        cur.close()
        conn.close()

        for row in new_rows:
            last_seen_id = max(last_seen_id, row["anomaly_id"])
            payload = json.dumps(row, default=str)
            for client in list(connected_clients):
                try:
                    await client.send_text(payload)
                except Exception:
                    connected_clients.remove(client)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(poll_for_new_anomalies())