import nbformat as nbf
import os

os.makedirs('notebooks', exist_ok=True)
os.makedirs('reports', exist_ok=True)

# --- 03 Benchmarks Notebook ---
nb3 = nbf.v4.new_notebook()
nb3.cells.append(nbf.v4.new_markdown_cell("# ETL Pipeline and Database Benchmarks"))
code3 = """import duckdb, time, os, sys
sys.path.append('..')
import matplotlib.pyplot as plt
from memory_profiler import memory_usage
from pipeline.load import load_facts_batched

con = duckdb.connect('../ecommerce.duckdb')

# --- BATCH SIZE BENCHMARKS ---
batch_sizes = [10_000, 50_000, 100_000, 500_000]
results = []

for bs in batch_sizes:
    # Re-create fact table empty for clean benchmark
    con.execute('DELETE FROM fact_events')
    start = time.perf_counter()
    # Call your load function with this batch size
    metrics = load_facts_batched(con, 'clean_oct', batch_size=bs)
    elapsed = time.perf_counter() - start
    rps = round(metrics['rows_loaded'] / elapsed) if elapsed > 0 else 0
    results.append({'batch': bs, 'seconds': round(elapsed,2), 'rows_per_sec': rps})
    print(f'Batch {bs:>8,}: {elapsed:.2f}s | {rps:,} rows/sec')

# Chart: Batch size vs throughput
plt.figure(figsize=(10,5))
plt.plot([r['batch'] for r in results], [r['rows_per_sec'] for r in results],
         marker='o', linewidth=2, color='steelblue')
plt.xlabel('Batch Size (rows)')
plt.ylabel('Throughput (rows/second)')
plt.title('Batch Size vs Insert Throughput')
plt.grid(True, alpha=0.3)
plt.tight_layout()
os.makedirs('../reports', exist_ok=True)
plt.savefig('../reports/batch_throughput.png', dpi=150)
plt.show()

# --- QUERY BENCHMARKS (with and without indexes) ---
queries = {
    'Q1_funnel':   'SELECT p.category_main, COUNT(*) FROM fact_events e JOIN dim_product p ON e.product_key=p.product_key GROUP BY 1 LIMIT 20',
    'Q2_session':  'SELECT user_session, COUNT(*) FROM fact_events GROUP BY user_session LIMIT 10',
    'Q3_brand':    'SELECT p.brand, SUM(e.price) FROM fact_events e JOIN dim_product p ON e.product_key=p.product_key WHERE e.event_type=\\'purchase\\' GROUP BY 1 LIMIT 10',
    'Q4_churn':    'SELECT COUNT(DISTINCT user_key) FROM fact_events WHERE event_month=10',
    'Q5_hourly':   'SELECT d.hour, COUNT(*) FROM fact_events e JOIN dim_date d ON e.date_key=d.date_key WHERE e.event_type=\\'purchase\\' GROUP BY 1 ORDER BY 1'
}

# Run without indexes
con.execute('DROP INDEX IF EXISTS idx_events_type')
con.execute('DROP INDEX IF EXISTS idx_events_user')
times_no_idx = {}
for name, sql in queries.items():
    start = time.perf_counter()
    con.execute(sql).fetchall()
    times_no_idx[name] = round(time.perf_counter() - start, 4)

# Recreate indexes
con.execute('CREATE INDEX idx_events_type ON fact_events(event_type)')
con.execute('CREATE INDEX idx_events_user ON fact_events(user_key)')
times_with_idx = {}
for name, sql in queries.items():
    start = time.perf_counter()
    con.execute(sql).fetchall()
    times_with_idx[name] = round(time.perf_counter() - start, 4)

# Chart: Query times with/without indexes
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
plt.savefig('../reports/query_benchmark.png', dpi=150)
plt.show()"""
nb3.cells.append(nbf.v4.new_code_cell(code3))
with open('notebooks/03_benchmarks.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb3, f)

# --- 04 Queries Notebook ---
nb4 = nbf.v4.new_notebook()
nb4.cells.append(nbf.v4.new_markdown_cell("# Analytical Queries (Section C)"))
code4 = """import duckdb
import pandas as pd
con = duckdb.connect('../ecommerce.duckdb')

queries = [
'''-- Query 1: Funnel Analysis
SELECT
  p.category_main,
  COUNT(DISTINCT CASE WHEN e.event_type='view'     THEN e.user_key END) AS viewers,
  COUNT(DISTINCT CASE WHEN e.event_type='cart'     THEN e.user_key END) AS carted,
  COUNT(DISTINCT CASE WHEN e.event_type='purchase' THEN e.user_key END) AS purchasers,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.event_type='cart' THEN e.user_key END)
    / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type='view' THEN e.user_key END),0), 2)
    AS view_to_cart_pct,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN e.event_type='purchase' THEN e.user_key END)
    / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type='cart' THEN e.user_key END),0), 2)
    AS cart_to_purchase_pct
FROM fact_events e
JOIN dim_product p ON e.product_key = p.product_key
GROUP BY p.category_main
ORDER BY viewers DESC
LIMIT 20;''',

'''-- Query 2: Session Aggregation
SELECT
  user_session,
  COUNT(*)                                            AS total_events,
  COUNT(DISTINCT product_key)                         AS distinct_products,
  SUM(CASE WHEN event_type='cart' THEN price ELSE 0 END) AS total_cart_value,
  MAX(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) AS had_purchase
FROM fact_events
GROUP BY user_session
ORDER BY total_events DESC
LIMIT 10;''',

'''-- Query 3: Top 10 Brands by Revenue per Month
WITH brand_revenue AS (
  SELECT
    e.event_month,
    p.brand,
    SUM(e.price) AS total_revenue,
    RANK() OVER (PARTITION BY e.event_month ORDER BY SUM(e.price) DESC) AS rnk
  FROM fact_events e
  JOIN dim_product p ON e.product_key = p.product_key
  WHERE e.event_type = 'purchase'
    AND p.brand IS NOT NULL
    AND p.brand != 'unknown'
  GROUP BY e.event_month, p.brand
)
SELECT event_month, brand, ROUND(total_revenue,2) AS total_revenue, rnk
FROM brand_revenue
WHERE rnk <= 10
ORDER BY event_month, rnk;''',

'''-- Query 4: October Buyers Who Did Not Return in November
SELECT DISTINCT oct.user_id
FROM (
  SELECT DISTINCT u.user_id
  FROM fact_events e
  JOIN dim_user u ON e.user_key = u.user_key
  WHERE e.event_type = 'purchase' AND e.event_month = 10
) oct
LEFT JOIN (
  SELECT DISTINCT u.user_id
  FROM fact_events e
  JOIN dim_user u ON e.user_key = u.user_key
  WHERE e.event_month = 11
) nov ON oct.user_id = nov.user_id
WHERE nov.user_id IS NULL
ORDER BY oct.user_id
LIMIT 10;''',

'''-- Query 5: Hourly Purchase Distribution
SELECT
  d.hour,
  COUNT(*) AS purchase_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM fact_events e
JOIN dim_date d ON e.date_key = d.date_key
WHERE e.event_type = 'purchase'
GROUP BY d.hour
ORDER BY d.hour;'''
]

for i, q in enumerate(queries, 1):
    print(f"\\n--- Running Query {i} ---")
    display(con.execute(q).df())
"""
nb4.cells.append(nbf.v4.new_code_cell(code4))
with open('notebooks/04_queries.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb4, f)

# --- 01 and 02 Stubs ---
nb1 = nbf.v4.new_notebook()
nb1.cells.append(nbf.v4.new_markdown_cell("# Schema Design"))
with open('notebooks/01_schema_design.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb1, f)

nb2 = nbf.v4.new_notebook()
nb2.cells.append(nbf.v4.new_markdown_cell("# ETL Pipeline"))
with open('notebooks/02_etl_pipeline.ipynb', 'w', encoding='utf-8') as f:
    nbf.write(nb2, f)

print("Notebooks generated successfully.")
