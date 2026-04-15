"""
extract.py - Stage 1: Extract data from raw CSV files into DuckDB
"""
import duckdb
import time
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('pipeline.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def extract(csv_path: str, con: duckdb.DuckDBPyConnection, table_name: str) -> dict:
    """
    Extract data from a CSV file into a DuckDB raw table.
    Returns a dict with extraction metrics.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f'CSV not found: {csv_path}')

    start_time = time.perf_counter()
    logger.info(f'Starting extraction: {csv_path}')

    # DuckDB reads CSV directly without loading into RAM
    # ON_ERROR='skip' handles malformed rows gracefully
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_csv(
            '{csv_path}',
            header=true,
            ignore_errors=true,
            types={{
                'event_time': 'TIMESTAMP',
                'event_type': 'VARCHAR',
                'product_id': 'BIGINT',
                'category_id': 'BIGINT',
                'category_code': 'VARCHAR',
                'brand': 'VARCHAR',
                'price': 'DOUBLE',
                'user_id': 'BIGINT',
                'user_session': 'VARCHAR'
            }}
        )
    """)

    end_time = time.perf_counter()
    elapsed = round(end_time - start_time, 2)

    row_count = con.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0]
    logger.info(f'Extracted {row_count:,} rows from {csv_path} in {elapsed}s')

    return {
        'file': csv_path,
        'table': table_name,
        'rows_extracted': row_count,
        'elapsed_seconds': elapsed,
        'start_time': start_time,
        'end_time': end_time
    }


if __name__ == '__main__':
    con = duckdb.connect('ecommerce.duckdb')
    metrics_oct = extract('data/raw/2019-Oct.csv', con, 'raw_oct')
    metrics_nov = extract('data/raw/2019-Nov.csv', con, 'raw_nov')
    print(f'Oct: {metrics_oct}')
    print(f'Nov: {metrics_nov}')
    con.close()
