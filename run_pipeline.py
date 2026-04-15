import duckdb
import logging
import os
from pipeline.load import load_dimensions, load_facts_batched

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Resuming Pipeline with Bulk Loading...")
    con = duckdb.connect('ecommerce.duckdb')
    
    # PERFORMANCE OPTIMIZATIONS
    con.execute("PRAGMA memory_limit='8GB'")
    con.execute("PRAGMA temp_directory='duckdb_temp'")
    con.execute("PRAGMA max_temp_directory_size='100GiB'")
    con.execute("SET preserve_insertion_order=false")
    
    logger.info("Step 1: Updating Dimension Tables (Fast)...")
    # Quick update ensure dimensions are fully populated
    load_dimensions(con, 'clean_oct')
    load_dimensions(con, 'clean_nov')
    
    logger.info("Step 2: Clearing Fact Table for Clean Load...")
    con.execute("DELETE FROM fact_events")
    
    logger.info("Step 3: Bulk Loading Fact Table (High Speed)...")
    # Load October then November
    load_facts_batched(con, 'clean_oct')
    load_facts_batched(con, 'clean_nov')
    
    logger.info("PIPELINE SUCCESSFULLY COMPLETED!")
    con.close()

if __name__ == '__main__':
    main()
