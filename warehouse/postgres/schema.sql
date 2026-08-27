-- RetailPulse app warehouse schema
-- Runs automatically on first postgres_app container start (via docker-entrypoint-initdb.d)

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- SILVER: products (dimension, slowly changing — SCD Type 1 is fine here,
-- since name/category rarely need history)
-- ============================================================
CREATE TABLE IF NOT EXISTS silver.products (
    product_id      SERIAL PRIMARY KEY,
    source_site     TEXT NOT NULL,
    source_url      TEXT NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    category        TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- SILVER: price_history (SCD Type 2)
-- Every price/stock change gets a NEW row. Old rows are closed out
-- by setting valid_to and is_current = false. Nothing is ever overwritten.
-- ============================================================
CREATE TABLE IF NOT EXISTS silver.price_history (
    price_history_id  BIGSERIAL PRIMARY KEY,
    product_id        INTEGER NOT NULL REFERENCES silver.products(product_id),
    price              NUMERIC(12, 2) NOT NULL,
    currency           TEXT NOT NULL DEFAULT 'INR',
    in_stock           BOOLEAN NOT NULL DEFAULT true,
    valid_from         TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to           TIMESTAMPTZ,               -- NULL while this is the current row
    is_current         BOOLEAN NOT NULL DEFAULT true,
    scrape_batch_id    TEXT NOT NULL              -- links back to the raw Bronze file in MinIO
);

CREATE INDEX IF NOT EXISTS idx_price_history_product_current
    ON silver.price_history (product_id, is_current);

CREATE INDEX IF NOT EXISTS idx_price_history_valid_range
    ON silver.price_history (product_id, valid_from, valid_to);

-- ============================================================
-- GOLD: daily price aggregates (built by the silver_to_gold Airflow DAG)
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.daily_price_summary (
    product_id      INTEGER NOT NULL REFERENCES silver.products(product_id),
    summary_date    DATE NOT NULL,
    min_price       NUMERIC(12, 2),
    max_price       NUMERIC(12, 2),
    avg_price       NUMERIC(12, 2),
    price_change_pct NUMERIC(6, 2),
    PRIMARY KEY (product_id, summary_date)
);

-- ============================================================
-- GOLD: anomalies (written by the anomaly_detector, read by the API/dashboard)
-- ============================================================
CREATE TABLE IF NOT EXISTS gold.anomalies (
    anomaly_id      BIGSERIAL PRIMARY KEY,
    product_id      INTEGER NOT NULL REFERENCES silver.products(product_id),
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    anomaly_type    TEXT NOT NULL,          -- e.g. 'PRICE_DROP', 'PRICE_SPIKE', 'STOCKOUT'
    magnitude_pct   NUMERIC(6, 2),
    narrative       TEXT,                    -- filled in later by the LLM layer
    resolved        BOOLEAN NOT NULL DEFAULT false
);
