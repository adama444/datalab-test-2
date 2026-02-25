# Exercise 2: Data Pipeline with NoSQL Data Source
## Project overview
This repository implements a containerized data pipeline that ingests heterogeneous NoSQL records (source JSON / MongoDB), applies schema normalization and validation, and produces analytics-ready datasets in a layered (Bronze / Silver / Gold) architecture.

## Architecture overview and why these choices

- **Containerized Spark (Docker Compose)**: Reproducible local cluster — Docker Compose lets us run a small Spark master/worker environment that mirrors production clusters. This simplifies development, testing, and CI while keeping deployments consistent.

- **Apache Spark (PySpark)**: Chosen for scalable ETL and robust handling of semi-structured data. Spark provides:
    - native support for evolving schemas and nested data structures,
    - efficient distributed processing for larger datasets,
    - easy integration with Parquet and S3-compatible stores.

- **MinIO / S3-compatible object store (local dev)**: S3 API compatibility gives production parity while remaining self-contained for local development. Object storage + Parquet is ideal for columnar I/O and incremental processing.

- **Medallion (Bronze / Silver / Gold) layering**: This pattern separates concerns:
    - Bronze: raw ingested data (exact snapshot of source) — traceability and replayability.
    - Silver: cleaned, normalized, and validated rows (business schema applied).
    - Gold: aggregated, analytics-ready tables or views for reporting/BI.

- **Parquet for persisted data**: Columnar format provides compression, predicate pushdown, and efficient analytics; it naturally complements Spark and S3-like storage.

- **Config-driven mapping (config/mapping_catalog.json)**: Keeps transformation rules and field mappings external to code so schema changes are handled by config updates rather than brittle code edits.

- **Separation of concerns (src/)**:
    - `src/ingestion.py` — fetches raw data and persists Bronze.
    - `src/transformation.py` — normalization and Silver logic.
    - `src/reporting.py` — Gold-level aggregations and writes to final targets.
Keeping these responsibilities separated improves testability and maintenance.

## How these choices map to the repo

- Source files: `src/ingestion.py`, `src/transformation.py`, `src/reporting.py`
- Configuration: `config/mapping_catalog.json`
- Local object-store helper: `scripts/upload_to_minio.py`
- Infrastructure: `docker-compose.yml`, `Dockerfile`, `requirements.txt`

## Practical reasons / trade-offs

- Scalability vs Complexity: Spark adds operational complexity but is justified if data volume or transformation complexity grows; for very small datasets, a lightweight runner (Pandas) could be used for prototyping.

- Local development parity: Docker + MinIO approximates production S3 and cluster behavior without external dependencies.

- Observability & data quality: Medallion layering plus explicit validation (e.g., Great Expectations if integrated later) keeps errors isolated and auditable.

## Quick start (development)

Start the stack:
```bash
docker-compose up -d
```

Example: run the reporting job inside the Spark master (matches how it's executed in this project):
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /app/src/reporting.py
```

Local script tests (non-cluster):
```bash
python3 src/ingestion.py    # run ingestion locally (reads config/data and writes Bronze)
python3 src/transformation.py
python3 src/reporting.py
```

## Next steps and recommendations

- Add a lightweight orchestration (Airflow / Prefect) for production DAGs and retries.
- Add basic metrics/monitoring (pipeline durations, error counts).

