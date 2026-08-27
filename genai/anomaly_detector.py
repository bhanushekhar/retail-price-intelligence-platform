"""
Statistics-first anomaly detection.

Deliberately simple and explainable before any ML: for every product,
compare its current price_history row against the row immediately before
it (linked by prev.valid_to == current.valid_from, which SCD2 sets
atomically). Flag PRICE_DROP / PRICE_SPIKE if the % change crosses a
threshold, or STOCKOUT if it went from in-stock to out-of-stock.

This function is idempotent: the UNIQUE (product_id, to_price_history_id)
constraint on gold.anomalies means re-running this never creates duplicate
anomaly rows for the same transition.
"""

PRICE_CHANGE_THRESHOLD_PCT = 5.0  # anything beyond +/-5% counts as an anomaly

DETECT_TRANSITIONS_SQL = """
SELECT
    cur_row.product_id,
    prev.price_history_id AS from_id, prev.price AS from_price, prev.in_stock AS from_stock,
    cur_row.price_history_id AS to_id, cur_row.price AS to_price, cur_row.in_stock AS to_stock
FROM silver.price_history cur_row
JOIN silver.price_history prev
    ON prev.product_id = cur_row.product_id
    AND prev.valid_to = cur_row.valid_from
WHERE cur_row.is_current = true
"""

INSERT_ANOMALY_SQL = """
INSERT INTO gold.anomalies
    (product_id, anomaly_type, magnitude_pct, from_price_history_id, to_price_history_id)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (product_id, to_price_history_id) DO NOTHING
RETURNING anomaly_id
"""


def detect_and_insert_anomalies(cur) -> dict:
    """
    Scans all current price transitions and inserts anomaly rows for the
    ones that cross the threshold. Returns counts by type for logging.
    """
    cur.execute(DETECT_TRANSITIONS_SQL)
    transitions = cur.fetchall()

    stats = {"PRICE_DROP": 0, "PRICE_SPIKE": 0, "STOCKOUT": 0, "skipped_no_anomaly": 0}

    for product_id, from_id, from_price, from_stock, to_id, to_price, to_stock in transitions:
        pct_change = round((float(to_price) - float(from_price)) / float(from_price) * 100, 2)

        if from_stock and not to_stock:
            anomaly_type, magnitude = "STOCKOUT", None
        elif pct_change <= -PRICE_CHANGE_THRESHOLD_PCT:
            anomaly_type, magnitude = "PRICE_DROP", pct_change
        elif pct_change >= PRICE_CHANGE_THRESHOLD_PCT:
            anomaly_type, magnitude = "PRICE_SPIKE", pct_change
        else:
            stats["skipped_no_anomaly"] += 1
            continue

        cur.execute(INSERT_ANOMALY_SQL, (product_id, anomaly_type, magnitude, from_id, to_id))
        if cur.fetchone():  # None if ON CONFLICT DO NOTHING skipped it (already existed)
            stats[anomaly_type] += 1

    return stats