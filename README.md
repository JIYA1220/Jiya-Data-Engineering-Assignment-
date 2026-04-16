# E-Commerce Behavior Data Engineering Pipeline
**KIE Square Analytics — Data Engineer Trainee Assignment 2026**

**Author:** Jiya Sharma  
**Dataset:** eCommerce Behavior Data — Multi-Category Store (Oct & Nov 2019)  
**Scale:** 109,806,507 rows | 14.6 GB raw data  
**Database:** DuckDB (local, serverless, no cloud required)

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Place raw CSVs (do NOT commit these — see .gitignore)
# data/raw/2019-Oct.csv
# data/raw/2019-Nov.csv

# 3. Run full pipeline (~2-3 hours for complete 110M row dataset)
python run_pipeline.py

# 4. Run analytical queries
python queries.py
```

---

## Repository Structure

```
jiya-sharma-data-engineering-assignment/
├── README.md
├── requirements.txt
├── run_pipeline.py              # Main orchestration script
├── queries.py                   # All 5 analytical queries
├── data/
│   └── raw/
│       └── .gitignore           # *.csv excluded — files too large for GitHub
├── schema/
│   ├── ddl.sql                  # All CREATE TABLE + CREATE INDEX statements
│   └── er_diagram.png           # Star schema ER diagram
├── pipeline/
│   ├── extract.py               # DuckDB vectorized CSV extraction
│   ├── transform.py             # Deduplication, validation, column derivation
│   └── load.py                  # Batch loading with idempotency
├── notebooks/
│   ├── 01_schema_design.ipynb   # Schema DDL and ER diagram
│   ├── 02_etl_pipeline.ipynb    # Full pipeline walkthrough
│   ├── 03_benchmarks.ipynb      # Performance benchmarks
│   └── 04_queries.ipynb         # All 5 queries with results
└── reports/
    └── performance_report.pdf   # Full technical report
```

---

## Schema Design

**Star Schema** — 1 fact table, 3 dimension tables.

| Table | Rows | Description |
|-------|------|-------------|
| `fact_events` | 109,806,507 | Core event log: view, cart, purchase |
| `dim_product` | ~500,000 | Product metadata with derived category columns |
| `dim_user` | ~7,000,000 | Unique user identifiers |
| `dim_date` | ~1,464 | Hourly time dimension for Oct–Nov 2019 |

**Why star schema?** Analytical queries (GROUP BY category, JOIN on product) require fewer joins against a denormalised star schema than a fully normalised 3NF model. DuckDB's columnar engine is specifically optimised for this pattern.

**Key design choices:**
- `category_code` split into `category_main` + `category_sub` during transform (eliminates need for separate `dim_category` table while preserving granularity)
- `event_month` added to `fact_events` for fast monthly filtering without a date dimension join
- Hash-based surrogate keys (`HASH(product_id)`) instead of `ROW_NUMBER()` — 4x faster at 110M row scale
- Indexes created **after** bulk load, not during — reduces load time by ~40%

---

## Performance Benchmarks

### Data Load Times

| Pipeline Stage | Rows | Time | Throughput |
|----------------|------|------|-----------|
| October Extraction | 42,448,764 | 31.58s | 1,344,166 rows/sec |
| November Extraction | 67,501,979 | 92.46s | 730,066 rows/sec |
| October Transformation | 42,413,557 | 446.84s | 94,917 rows/sec |
| November Transformation | 67,392,950 | 591.11s | 114,010 rows/sec |
| Dimension Loading | ~110,000,000 | 310.85s | 353,865 rows/sec |
| Fact Load (Total) | 109,806,507 | 6,158.39s | 17,830 rows/sec |

### Batch Size vs Insert Throughput

| Batch Size | Throughput (rows/sec) | Notes |
|------------|----------------------|-------|
| 10,000 | 92,725 | Transaction overhead dominates |
| 50,000 | 996,256 | Good — fits L3 cache |
| **250,000** | **1,434,894** | **OPTIMAL for this hardware** |

### Query Execution Time — With vs Without Indexes

| Query | Without Index | With Index | Speedup |
|-------|--------------|------------|---------|
| Q1: Funnel Analysis | 3.97s | 1.24s | **3.2x** |
| Q2: Session Aggregation | 3.87s | 2.15s | **1.8x** |
| Q3: Brand Revenue | 3.78s | 0.84s | **4.5x** |
| Q4: Churn Analysis | 2.56s | 0.42s | **6.1x** |
| Q5: Hourly Distribution | 0.91s | 0.38s | **2.4x** |

---

## Data Quality Summary

| Check | Rows Affected | Action |
|-------|--------------|--------|
| NULL mandatory fields | ~35,207 | Dropped |
| Duplicate events | 35,207 | Dropped (kept first) |
| Price anomalies (>$5,000 or <$0) | Flagged | Price set to NULL |
| Timestamp out of range | Minimal | Flagged in logs |
| **Clean rows loaded** | **109,806,507** | **Pass rate: 99.97%** |

**Idempotency test:** PASS — running pipeline twice produces identical row counts.

---

## Schema Design Justification (Section A3)

The Star Schema was chosen over a fully normalised 3NF model because the assignment's analytical queries (funnel analysis, session aggregation, brand revenue) all involve aggregations across dimensional attributes. A star schema minimises the number of joins required — queries touch the fact table and at most two dimensions, which is significantly faster than navigating a deeply normalised schema with multiple join hops.

The `dim_product` table contains `category_main` and `category_sub` as derived columns split from `category_code`. This is a deliberate denormalisation: creating a separate `dim_category` table would add a join without meaningful benefit because category hierarchies in the raw data are not reused across products in a normalised way.

Indexes were chosen based on query selectivity analysis. The `event_type` index has the highest impact because it appears in every Section C query. The `user_key` index delivers the largest speedup (6.1x on Q4) because the churn analysis requires matching users across two month-specific subsets of 55M rows each. The `event_month` index reduces scan size from 110M to ~55M for all monthly-filtered queries.

---

## Scalability: What Would Change at 1TB+

1. **Compute:** Apache Spark on AWS EMR — distributes processing across a cluster. DuckDB transformation logic maps directly to Spark DataFrame API.
2. **Storage:** Apache Parquet partitioned by `event_month`/`event_date` — 5-8x I/O reduction vs CSV, columnar reads for analytics.
3. **Orchestration:** Apache Airflow DAGs with retry logic, backfill capability, and monitoring dashboards.
4. **Warehouse:** Google BigQuery or Amazon Redshift — distributed columnar stores that scale to petabytes. Same star schema DDL with minor syntax changes.

---

## Assumptions and Known Limitations

- Raw CSV files are excluded from the repo (sizes exceed GitHub limits). The `.gitignore` in `data/raw/` excludes all CSV files.
- All file paths are relative — no absolute paths hardcoded.
- The `product_key` is derived via `HASH(product_id)` which assumes product_id uniquely identifies a product. If the same product_id appears with different brand or category values across months (data quality issue in source), only the first occurrence is retained in `dim_product`.
- Execution time for full pipeline is approximately 2-3 hours on consumer hardware (8-core CPU, 16GB RAM). The `PRAGMA threads=4` setting can be increased on higher-core systems.

---

*Pipeline log output is written to `pipeline.log` in the project root.*
