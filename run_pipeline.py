import duckdb
import logging
import time
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting BULLETPROOF LOAD (Day-by-Day Chunking)...")
    con = duckdb.connect('ecommerce.duckdb')
    
    con.execute("PRAGMA memory_limit='8GB'")
    con.execute("PRAGMA temp_directory='duckdb_temp'")
    con.execute("SET preserve_insertion_order=false")
    os.makedirs('duckdb_temp', exist_ok=True)
    
    # 2. Cleanup & Schema
    logger.info("Recreating tables...")
    con.execute("DROP TABLE IF EXISTS fact_events; DROP TABLE IF EXISTS dim_product; DROP TABLE IF EXISTS dim_user; DROP TABLE IF EXISTS dim_date;")
    with open('schema/ddl.sql', 'r') as f:
        con.execute(f.read())

    # 3. Create Sequence
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_event_id START 1")

    # 4. Load Dimensions
    logger.info("Loading Dimension Tables...")
    con.execute("""
        INSERT INTO dim_date
        SELECT CAST(STRFTIME(h, '%Y%m%d%H') AS INTEGER), h, YEAR(h), MONTH(h), DAY(h), HOUR(h), DAYOFWEEK(h), DAYOFWEEK(h) IN (0,6)
        FROM (SELECT DISTINCT date_trunc('hour', event_time) h FROM (SELECT event_time FROM clean_oct UNION ALL SELECT event_time FROM clean_nov))
    """)
    con.execute("""
        INSERT INTO dim_product
        SELECT DISTINCT ON (product_id % 2147483647) CAST(product_id % 2147483647 AS INTEGER), product_id, category_id, category_code, category_main, category_sub, brand
        FROM (SELECT * FROM clean_oct UNION ALL SELECT * FROM clean_nov)
    """)
    con.execute("""
        INSERT INTO dim_user
        SELECT DISTINCT ON (user_id % 2147483647) CAST(user_id % 2147483647 AS INTEGER), user_id
        FROM (SELECT user_id FROM clean_oct UNION ALL SELECT user_id FROM clean_nov)
    """)

    # 5. DROP INDEXES
    con.execute("DROP INDEX IF EXISTS idx_events_type; DROP INDEX IF EXISTS idx_events_session; DROP INDEX IF EXISTS idx_events_user; DROP INDEX IF EXISTS idx_events_month; DROP INDEX IF EXISTS idx_events_product;")

    # 6. DAY-BY-DAY FACT LOAD (The ultimate memory fix)
    # Splitting the massive 110M row transaction into 61 tiny daily chunks
    for month, days in [('oct', 31), ('nov', 30)]:
        logger.info(f"Loading {month.upper()} Fact Table (Day by Day)...")
        start = time.perf_counter()
        
        for day in range(1, days + 1):
            con.execute(f"""
                INSERT INTO fact_events
                SELECT
                    nextval('seq_event_id'),
                    CAST(STRFTIME(event_time,'%Y%m%d%H') AS INTEGER),
                    CAST(user_id % 2147483647 AS INTEGER),
                    CAST(product_id % 2147483647 AS INTEGER),
                    event_type, price, user_session, event_month
                FROM clean_{month}
                WHERE user_session IS NOT NULL AND DAY(event_time) = {day}
            """)
            if day % 5 == 0:
                logger.info(f"  -> Loaded day {day}/{days}")
                
        elapsed = round(time.perf_counter() - start, 2)
        logger.info(f"{month.upper()} Load Complete in {elapsed}s")

    # 7. RECREATE INDEXES
    logger.info("Recreating indexes...")
    con.execute("CREATE INDEX idx_events_type ON fact_events(event_type)")
    con.execute("CREATE INDEX idx_events_user ON fact_events(user_key)")
    con.execute("CREATE INDEX idx_events_month ON fact_events(event_month)")

    logger.info("PIPELINE FULLY COMPLETE! RUN YOUR NOTEBOOKS NOW!")
    con.close()

if __name__ == '__main__':
    main()
