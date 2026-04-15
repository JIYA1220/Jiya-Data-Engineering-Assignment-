"""
load.py - Stage 3: High-Performance Bulk Load
"""
import duckdb
import time
import logging

logger = logging.getLogger(__name__)

def load_dimensions(con: duckdb.DuckDBPyConnection, clean_table: str) -> dict:
    """Load dimension tables from clean data. Idempotent via INSERT OR IGNORE."""
    start = time.perf_counter()

    # dim_date
    con.execute(f"""
        INSERT OR IGNORE INTO dim_date
        SELECT DISTINCT
            CAST(STRFTIME(event_time, '%Y%m%d%H') AS INTEGER) AS date_key,
            event_time                                         AS full_ts,
            YEAR(event_time)                                   AS year,
            MONTH(event_time)                                  AS month,
            DAY(event_time)                                    AS day,
            HOUR(event_time)                                   AS hour,
            DAYOFWEEK(event_time)                              AS day_of_week,
            DAYOFWEEK(event_time) IN (0,6)                     AS is_weekend
        FROM {clean_table}
    """)

    # dim_product (Using a more stable key generation)
    con.execute(f"""
        INSERT OR IGNORE INTO dim_product
        SELECT DISTINCT
            (product_id % 2147483647) AS product_key,
            product_id, category_id, category_code,
            category_main, category_sub, brand
        FROM {clean_table}
    """)

    # dim_user
    con.execute(f"""
        INSERT OR IGNORE INTO dim_user
        SELECT DISTINCT
            (user_id % 2147483647) AS user_key,
            user_id
        FROM {clean_table}
    """)

    elapsed = round(time.perf_counter() - start, 2)
    logger.info(f'Dimensions updated from {clean_table} in {elapsed}s')
    return {'elapsed_seconds': elapsed}


def load_facts_batched(con: duckdb.DuckDBPyConnection, clean_table: str, batch_size=None) -> dict:
    """
    RESCUE VERSION: Performs a high-speed BULK INSERT.
    """
    start = time.perf_counter()
    logger.info(f'Starting bulk load for {clean_table}...')
    
    # We use a single JOINed INSERT which DuckDB optimizes perfectly
    con.execute(f"""
        INSERT OR IGNORE INTO fact_events
        SELECT
            c.event_id,
            CAST(STRFTIME(c.event_time,'%Y%m%d%H') AS INTEGER) AS date_key,
            u.user_key,
            p.product_key,
            c.event_type,
            c.price,
            c.user_session,
            c.event_month
        FROM {clean_table} c
        JOIN dim_user    u ON c.user_id    = u.user_id
        JOIN dim_product p ON c.product_id = p.product_id
    """)

    elapsed = round(time.perf_counter() - start, 2)
    rows_loaded = con.execute(f'SELECT COUNT(*) FROM {clean_table}').fetchone()[0]
    throughput = round(rows_loaded / elapsed) if elapsed > 0 else 0

    logger.info(f'Bulk load complete for {clean_table}: {rows_loaded:,} rows in {elapsed}s ({throughput:,} rows/sec)')
    return {
        'rows_loaded': rows_loaded,
        'elapsed_seconds': elapsed,
        'throughput_rows_per_sec': throughput
    }
