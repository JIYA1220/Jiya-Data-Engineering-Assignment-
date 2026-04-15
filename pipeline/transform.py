"""
transform.py - Stage 2: Clean, validate and transform raw data (Optimised)
"""
import duckdb
import time
import logging

logger = logging.getLogger(__name__)

PRICE_MIN = 0.0
PRICE_MAX = 10000.0
DATE_MIN  = '2019-10-01'
DATE_MAX  = '2019-11-30'

def transform(con: duckdb.DuckDBPyConnection, source_table: str,
               output_table: str) -> dict:
    """
    Transform raw events: deduplicate, parse timestamps, derive columns,
    validate quality, handle NULLs. Uses a single pass for efficiency.
    """
    start = time.perf_counter()
    logger.info(f'Starting optimized transform: {source_table} -> {output_table}')

    # Count input rows
    raw_count = con.execute(f'SELECT COUNT(*) FROM {source_table}').fetchone()[0]
    
    # SINGLE-PASS TRANSFORMATION
    # We combine filtering NULLs, deduplication, and anomaly flagging into one query
    con.execute(f"""
        CREATE OR REPLACE TABLE {output_table} AS
        WITH filtered AS (
            SELECT * FROM {source_table}
            WHERE event_time IS NOT NULL AND event_type IS NOT NULL 
              AND user_id IS NOT NULL AND product_id IS NOT NULL
        ),
        deduped AS (
            SELECT DISTINCT ON (user_id, product_id, event_time, event_type) * 
            FROM filtered
        )
        SELECT
            CAST(ROW_NUMBER() OVER () AS BIGINT) AS event_id,
            event_time,
            event_type,
            product_id,
            category_id,
            COALESCE(category_code, 'uncategorized')   AS category_code,
            SPLIT_PART(COALESCE(category_code,'uncategorized'), '.', 1) AS category_main,
            SPLIT_PART(COALESCE(category_code,'uncategorized'), '.', 2) AS category_sub,
            COALESCE(brand, 'unknown')                 AS brand,
            CASE WHEN (price < {PRICE_MIN} OR price > {PRICE_MAX}) THEN NULL ELSE price END AS price,
            user_id,
            user_session,
            MONTH(event_time)                          AS event_month,
            (price < {PRICE_MIN} OR price > {PRICE_MAX}) AS price_anomaly
        FROM deduped
    """)

    clean_count = con.execute(f'SELECT COUNT(*) FROM {output_table}').fetchone()[0]
    elapsed = round(time.perf_counter() - start, 2)
    
    pass_rate = round(clean_count / raw_count * 100, 2) if raw_count > 0 else 0
    logger.info(f'Transform complete for {source_table}: {clean_count:,} rows in {elapsed}s. Pass rate: {pass_rate}%')
    
    return {
        'source': source_table,
        'output': output_table,
        'rows_output': clean_count,
        'pass_rate_pct': pass_rate,
        'elapsed_seconds': elapsed
    }
