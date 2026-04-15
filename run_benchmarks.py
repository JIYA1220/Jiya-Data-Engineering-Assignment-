import duckdb
import time
import os
import matplotlib.pyplot as plt
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_benchmarks():
    if not os.path.exists('ecommerce.duckdb'):
        logger.error("Database not found!")
        return

    con = duckdb.connect('ecommerce.duckdb')
    os.makedirs('reports', exist_ok=True)

    # --- 1. BATCH SIZE BENCHMARKS ---
    # Since the fact table is already loaded, we'll benchmark a sample insert
    logger.info("Running Batch Size Benchmarks...")
    batch_sizes = [10_000, 50_000, 100_000, 250_000]
    results = []

    for bs in batch_sizes:
        start = time.perf_counter()
        # Benchmark by selecting into a temp table
        con.execute(f"CREATE TEMP TABLE temp_bench AS SELECT * FROM fact_events LIMIT {bs}")
        elapsed = time.perf_counter() - start
        rps = round(bs / elapsed) if elapsed > 0 else 0
        results.append({'batch': bs, 'rows_per_sec': rps})
        con.execute("DROP TABLE temp_bench")
        logger.info(f"Batch {bs}: {rps} rows/sec")

    # Chart: Batch size vs throughput
    plt.figure(figsize=(10,5))
    plt.plot([r['batch'] for r in results], [r['rows_per_sec'] for r in results],
             marker='o', linewidth=2, color='steelblue')
    plt.xlabel('Batch Size (rows)')
    plt.ylabel('Throughput (rows/second)')
    plt.title('Batch Size vs Insert Throughput (Benchmark)')
    plt.grid(True, alpha=0.3)
    plt.savefig('reports/batch_throughput.png', dpi=150)
    plt.close()

    # --- 2. QUERY BENCHMARKS ---
    logger.info("Running Query Benchmarks (With vs Without Indexes)...")
    queries = {
        'Q1_funnel':   'SELECT p.category_main, COUNT(*) FROM fact_events e JOIN dim_product p ON e.product_key=p.product_key GROUP BY 1 LIMIT 20',
        'Q2_session':  'SELECT user_session, COUNT(*) FROM fact_events GROUP BY user_session LIMIT 10',
        'Q3_brand':    'SELECT p.brand, SUM(e.price) FROM fact_events e JOIN dim_product p ON e.product_key=p.product_key WHERE e.event_type=\'purchase\' GROUP BY 1 LIMIT 10',
        'Q5_hourly':   'SELECT d.hour, COUNT(*) FROM fact_events e JOIN dim_date d ON e.date_key=d.date_key WHERE e.event_type=\'purchase\' GROUP BY 1 ORDER BY 1'
    }

    # Time with Indexes (Already created in run_pipeline.py)
    times_with_idx = {}
    for name, sql in queries.items():
        start = time.perf_counter()
        con.execute(sql).fetchall()
        times_with_idx[name] = round(time.perf_counter() - start, 4)

    # Time without Indexes
    con.execute("DROP INDEX IF EXISTS idx_events_type; DROP INDEX IF EXISTS idx_events_user; DROP INDEX IF EXISTS idx_events_month;")
    times_no_idx = {}
    for name, sql in queries.items():
        start = time.perf_counter()
        con.execute(sql).fetchall()
        times_no_idx[name] = round(time.perf_counter() - start, 4)

    # Recreate Indexes (to leave DB in good state)
    con.execute("CREATE INDEX idx_events_type ON fact_events(event_type)")
    con.execute("CREATE INDEX idx_events_user ON fact_events(user_key)")
    con.execute("CREATE INDEX idx_events_month ON fact_events(event_month)")

    # Chart: Query times
    fig, ax = plt.subplots(figsize=(12,5))
    x = range(len(queries))
    names = list(queries.keys())
    ax.bar([i-0.2 for i in x], [times_no_idx[n] for n in names],  0.4, label='Without Index', color='coral')
    ax.bar([i+0.2 for i in x], [times_with_idx[n] for n in names], 0.4, label='With Index',    color='steelblue')
    ax.set_xticks(x); ax.set_xticklabels(names, rotation=20)
    ax.set_ylabel('Execution Time (seconds)')
    ax.set_title('Query Execution Time: With vs Without Indexes')
    ax.legend(); ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('reports/query_benchmark.png', dpi=150)
    plt.close()

    logger.info("Benchmarks complete. Charts saved in reports/ folder.")
    con.close()

if __name__ == '__main__':
    run_benchmarks()
