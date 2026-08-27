# RetailPulse — Real-Time Competitive Intelligence & AIOps Platform

A local, fully free e-commerce price-intelligence platform: RPA scraping →
distributed queue → data lake → Airflow ETL (SCD Type-2) → SQL/NoSQL
warehouse → GenAI anomaly explanations → FastAPI/WebSocket → live dashboard.

See `docs/architecture.png` for the full system diagram.

## Prerequisites (Windows, 16GB+ RAM)

- Docker Desktop (already installed ✅)
- Python 3.11+ — [python.org](https://www.python.org/downloads/) (check "Add to PATH" during install)
- Git — [git-scm.com](https://git-scm.com/download/win)
- A terminal: PowerShell (built-in) or Windows Terminal

## Step 1 — Clone / initialize the repo

```powershell
cd C:\Users\<you>\Projects
git init retailpulse
cd retailpulse
# copy all the project files into this folder
```

## Step 2 — Python environment

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

## Step 3 — Environment variables

```powershell
copy .env.example .env
```
Leave the defaults as-is for now — they match the docker-compose.yml services.

## Step 4 — Start the infrastructure

```powershell
docker compose up -d
```

This starts, in the background:
| Service | URL | Purpose |
|---|---|---|
| Postgres (app) | localhost:5432 | Silver/Gold warehouse, SCD2 price history |
| MongoDB | localhost:27017 | Raw product metadata |
| Redis | localhost:6379 | Queue broker |
| MinIO | http://localhost:9001 | Data lake console (login: retailpulse / retailpulse123) |
| Airflow | http://localhost:8080 | ETL orchestration (login: admin / admin) |

First boot takes 1-3 minutes (Airflow has to initialize its metadata DB).
Check progress with:
```powershell
docker compose logs -f airflow-init
```

## Step 5 — Verify everything is up

```powershell
docker compose ps
```
All services should show `Up` (or `healthy`). Then open:
- http://localhost:9001 → MinIO console → create a bucket named `retailpulse-lake`
- http://localhost:8080 → Airflow UI → log in with admin/admin

## Step 6 — Run your first scraper

Edit `rpa/scrapers/site_a_scraper.py`:
- Replace `PRODUCT_URLS` with real product page URLs (start with a
  practice/demo e-commerce site, or your own test storefront)
- Replace the placeholder CSS selectors (`h1.product-title`, `.price`,
  `.stock-status`) with the real selectors from that site (use browser
  DevTools → right-click element → Inspect to find them)

Then run it:
```powershell
python rpa\scrapers\site_a_scraper.py
```

You should see output like:
```
OK   https://example.com/product/1 -> ₹1299.0
Wrote 2 records to bronze_local\site_a\site_a_ab12cd34.json
```

That JSON file is your **Bronze layer**, proving the RPA ingestion works
end-to-end before anything else is wired up.

## What's next (in build order)

1. ✅ RPA scraper writing raw JSON locally (you just did this)
2. Push scraped payloads to MinIO instead of local disk
3. Add Redis Streams + a Celery worker to process payloads in parallel
4. Write the first Airflow DAG: Bronze (MinIO) → Silver (Postgres, SCD2)
5. Add the Silver → Gold aggregation DAG
6. Build the anomaly detector + LLM narrative layer
7. Build the FastAPI REST + WebSocket service
8. Build the Streamlit dashboard

Ask Claude for the next file (e.g. "give me the MinIO upload code" or
"give me the first Airflow DAG") whenever you're ready for the next step —
build one piece at a time and get each one actually running before moving on.

## Stopping everything

```powershell
docker compose down          # stops containers, keeps data
docker compose down -v       # stops containers AND deletes all data (fresh start)
```
