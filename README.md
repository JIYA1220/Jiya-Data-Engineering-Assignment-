# Jiya Sharma - [Roll Number] - [Branch]

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
4. Run notebooks in the `notebooks/` directory to reproduce benchmarks and queries.

## Performance Benchmarks

### Data Load Times
| File         | Rows        | Load Time | Throughput       |
|--------------|-------------|-----------|-----------------|
| 2019-Oct.csv | XX,XXX,XXX  | XXs       | XX,XXX rows/sec |
| 2019-Nov.csv | XX,XXX,XXX  | XXs       | XX,XXX rows/sec |
| Combined     | XX,XXX,XXX  | XXs       | XX,XXX rows/sec |

### Batch Size vs Throughput
| Batch Size | Rows/Second | Total Time |
|------------|-------------|------------|
| 10,000     | XX,XXX      | XXs        |
| 50,000     | XX,XXX      | XXs        |
| 100,000    | XX,XXX      | XXs        |
| 500,000    | XX,XXX      | XXs        |

### Query Performance (with vs without indexes)
| Query | Without Index | With Index | Speedup |
|-------|---------------|------------|---------|
| Q1    | Xs            | Xs         | Xx      |
| Q2    | Xs            | Xs         | Xx      |
| Q3    | Xs            | Xs         | Xx      |
| Q4    | Xs            | Xs         | Xx      |
| Q5    | Xs            | Xs         | Xx      |

## Schema Design Decisions
[To be filled - A3 Justification]

## Known Limitations
* `product_key` lookup currently uses `product_id` which might not be globally unique across categories if data structure changes.
* Incremental loads assume exact file paths and idempotency is managed by `INSERT OR IGNORE`.
