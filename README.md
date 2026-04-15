# Jiya Sharma - [Roll Number] - [Branch]

## Project Overview
This project implements a high-performance Data Engineering pipeline for processing over 110 million rows of e-commerce behavior data using DuckDB and Python.

## Setup Instructions
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Place the raw datasets `2019-Oct.csv` and `2019-Nov.csv` inside `data/raw/`.
3. Run the ETL pipeline:
   ```bash
   python run_pipeline.py
   ```
4. Run benchmarks and queries:
   ```bash
   python run_benchmarks.py
   ```

## Performance Benchmarks

### Data Load Times
| File         | Rows        | Load Time | Throughput       |
|--------------|-------------|-----------|-----------------|
| 2019-Oct.csv | 42,413,557  | 1132.81s  | 37,441 rows/sec |
| 2019-Nov.csv | 67,392,950  | 5025.58s  | 13,410 rows/sec |
| **Combined** | **109,806,507** | **6158.39s** | **17,830 rows/sec** |

### Batch Size vs Throughput
| Batch Size | Rows/Second |
|------------|-------------|
| 10,000     | 92,725      |
| 50,000     | 996,256     |
| 100,000    | 1,074,273   |
| 250,000    | 1,434,894   |

## Schema Design Decisions (A3 Justification)
I implemented a **Star Schema** with one central fact table (`fact_events`) and three dimension tables (`dim_user`, `dim_product`, `dim_date`). 

* **Normalization:** The schema follows a 3NF-inspired structure for dimensions to reduce redundancy, while keeping the fact table extremely narrow (8 columns) to optimize for massive scans.
* **Indexing:** B-Tree indexes were applied to `event_type`, `user_key`, and `event_month`. These were specifically chosen to speed up the Funnel Analysis and Monthly Churn queries by allowing the engine to skip non-relevant data pages.
* **Data Integrity:** Used `NOT NULL` constraints on surrogate keys and `user_session` to ensure analytical reliability.

## Project Summary (D2)
* **Design Decisions:** Used DuckDB for its vectorized execution engine, which outperformed traditional row-based databases (like SQLite) by over 10x for this 110M row dataset.
* **Bottlenecks:** The primary bottleneck was memory exhaustion during the "Commit" phase of large transactions. This was solved by implementing a **Day-by-Day chunked loading strategy**, which kept memory usage stable under 8GB throughout the 110M row load.
* **1TB Scaling:** For a 1TB dataset, I would move to a distributed architecture using **Apache Spark** on AWS EMR, storing data in **Parquet** format on S3 partitioned by `event_month` and `event_day`.

## Known Limitations
* `product_key` is generated via a hash of `product_id`; in a production system, a true auto-incrementing surrogate key from a metadata store would be preferred.
* Memory limits are hard-coded to 8GB; production scripts should dynamically detect available system RAM.
