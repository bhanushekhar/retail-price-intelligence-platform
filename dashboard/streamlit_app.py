"""
RetailPulse Dashboard — Streamlit frontend.

Run (from the retailpulse/ root, with your venv active, API server already
running via `python -m uvicorn api.main:app --port 8000` in another terminal):
    streamlit run dashboard/streamlit_app.py

Design note: Streamlit reruns the whole script on every refresh rather than
running a persistent event loop, so consuming the /ws/prices WebSocket
directly gets awkward here. Instead this dashboard auto-refreshes every
few seconds and re-polls the REST endpoints — simpler and more robust for
Streamlit's execution model, at the cost of a few seconds of latency
versus a true push-based frontend (which is what /ws/prices is for,
e.g. a React client).
"""

import os
import sys
import json
from pathlib import Path

import requests
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# Streamlit only puts the script's own folder (dashboard/) on sys.path,
# not the project root — so we add the root manually here to be able to
# import rpa.scrapers.site_a_scraper below.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from rpa.scrapers.site_a_scraper import scrape_records

API_BASE = os.environ.get("RETAILPULSE_API", "http://localhost:8000")

st.set_page_config(page_title="RetailPulse", page_icon="📊", layout="wide")

# Auto-refresh the whole app every 5 seconds so new scrapes/anomalies show
# up without the user manually reloading the page.
st_autorefresh(interval=5000, key="auto_refresh")

st.title("📊 RetailPulse — Live Price Intelligence")
st.caption("RPA ingestion -> distributed queue -> data lake -> Airflow ETL (SCD2) -> GenAI anomaly detection -> here")


def fetch(path: str):
    try:
        resp = requests.get(f"{API_BASE}{path}", timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        st.error(f"Could not reach API at {API_BASE}{path} — is uvicorn running? ({exc})")
        return None


# ============================================================
# Section 0: Run a new scrape from an uploaded JSON file
# ============================================================
st.header("▶ Run New Scrape")

with st.expander("Upload a URL list and run the scraper", expanded=False):
    st.caption(
        'Upload a JSON file: a list of objects like '
        '`{"url": "https://...", "source_name": "bookscrape"}`. '
        "URLs are grouped by source_name and each group is enqueued as its own batch."
    )
    uploaded_file = st.file_uploader("Input JSON file", type=["json"])

    if uploaded_file is not None:
        try:
            records = json.load(uploaded_file)
            if not isinstance(records, list):
                st.error("File must contain a JSON array of {url, source_name} objects.")
                records = None
            else:
                bad = [r for r in records if "url" not in r or "source_name" not in r]
                if bad:
                    st.error(f"{len(bad)} record(s) missing 'url' or 'source_name'.")
                    records = None
                else:
                    st.success(f"Loaded {len(records)} valid records.")
                    st.dataframe(pd.DataFrame(records), use_container_width=True, hide_index=True)
        except json.JSONDecodeError as exc:
            st.error(f"Invalid JSON: {exc}")
            records = None

        if records and st.button("Run Scrape", type="primary"):
            with st.spinner(f"Scraping {len(records)} URLs... this runs synchronously and may take a moment."):
                summary = scrape_records(records)
            st.success("Scrape complete — batches enqueued to the Celery worker.")
            st.json(summary)
            st.caption(
                "Next: trigger bronze_to_silver_books_toscrape (or the relevant DAG) "
                "in Airflow to load this new data into Postgres."
            )


# ============================================================
# Section 1: Current prices
# ============================================================
st.header("Tracked Products")

products = fetch("/products")
if products:
    df = pd.DataFrame(products)
    df["valid_from"] = pd.to_datetime(df["valid_from"]).dt.strftime("%Y-%m-%d %H:%M")
    display_df = df[["product_id", "title", "price", "currency", "in_stock", "valid_from"]]
    display_df.columns = ["ID", "Title", "Price", "Currency", "In Stock", "Last Updated"]
    st.dataframe(display_df, use_container_width=True, hide_index=True)
else:
    st.info("No products found yet — run the scraper and the Bronze->Silver DAG first.")


# ============================================================
# Section 2: Price history chart for a selected product
# ============================================================
st.header("Price History")

if products:
    product_options = {f"{p['title']} (ID {p['product_id']})": p["product_id"] for p in products}
    selected_label = st.selectbox("Select a product", list(product_options.keys()))
    selected_id = product_options[selected_label]

    history = fetch(f"/products/{selected_id}/history")
    if history:
        hist_df = pd.DataFrame(history)
        hist_df["valid_from"] = pd.to_datetime(hist_df["valid_from"])
        chart_df = hist_df.set_index("valid_from")[["price"]]
        st.line_chart(chart_df, use_container_width=True)

        with st.expander("Raw price history (every SCD2 row)"):
            st.dataframe(
                hist_df[["price_history_id", "price", "in_stock", "valid_from", "valid_to", "is_current"]],
                use_container_width=True,
                hide_index=True,
            )
else:
    st.info("No products to chart yet.")


# ============================================================
# Section 3: Detected anomalies with LLM narratives
# ============================================================
st.header("🚨 Detected Anomalies")

anomalies = fetch("/anomalies")
if anomalies:
    for a in anomalies:
        icon = {"PRICE_DROP": "📉", "PRICE_SPIKE": "📈", "STOCKOUT": "🛑"}.get(a["anomaly_type"], "⚠️")
        with st.container(border=True):
            col1, col2 = st.columns([1, 4])
            with col1:
                st.markdown(f"### {icon} {a['anomaly_type']}")
                if a["magnitude_pct"] is not None:
                    st.metric("Change", f"{a['magnitude_pct']}%")
            with col2:
                st.markdown(f"**{a['title']}**")
                st.write(a["narrative"] or "_Narrative generating..._")
                st.caption(f"Detected at {a['detected_at']}")
else:
    st.info("No anomalies detected yet — trigger the anomaly_detection_and_narrative DAG in Airflow.")