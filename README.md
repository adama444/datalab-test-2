# Exercise 2: Data Pipeline with NoSQL Data Source
## Project overview
This repository implements an end-to-end data pipeline designed to centralize and normalize public service reports (waste management, street lighting, road maintenance) in Togo. It transforms heterogeneous NoSQL records into validated, analytics-ready datasets for urban decision-making.

## Architecture overview and technical choices
### 1. Medallion Layering (Bronze / Silver / Gold)
- **Bronze (Raw)**: Ingests raw JSON records into MinIO. This ensures full data traceability and the ability to replay the pipeline from source.
- **Silver (Cleaned)**: Applies dynamic normalization via PySpark. Data is cleaned (date formats standardized, field mapping applied) and stored in Parquet format for optimized query performance.
- **Gold (Analytics)**: Exports the refined data to PostgreSQL + PostGIS. Data is structured within a schema and ready for immediate consumption by Apache Superset.

### 2. Technology Stack
- **Containerization (Docker Compose)**: Reproducible local cluster — Docker Compose lets us run a small Spark master/worker environment that mirrors production clusters. This simplifies development, testing, and CI while keeping deployments consistent.

- **Orchestration**: Apache Airflow. Automates the task sequence: Ingestion → Transformation → Gold Loading.
- **Processing Engine**: Apache Spark (PySpark). Selected for its ability to handle evolving schemas and distributed processing. Spark provides:
    - native support for evolving schemas and nested data structures,
    - efficient distributed processing for larger datasets,
    - easy integration with Parquet and S3-compatible stores.

- **Object Storage**: MinIO. Provides an S3-compatible API for local development with production parity.
- **Database**: PostgreSQL with PostGIS extension. Manages geographical coordinates for service requests across Togolese communes (e.g., Lomé, Aného).
- **BI & Visualization**: Apache Superset. Powers interactive dashboards for operational monitoring.

- **Parquet for persisted data**: Columnar format provides compression, predicate pushdown, and efficient analytics; it naturally complements Spark and S3-like storage.

### 3. Config-Driven Mapping
The pipeline uses a mapping_catalog.json file to externalize transformation rules. This allows for field updates (e.g., adding a new source field for a district) without modifying the Python source code, making the system resilient to NoSQL schema drift.It Keeps transformation rules and field mappings external to code so schema changes are handled by config updates rather than brittle code edits.

## Practical reasons / trade-offs
- Scalability vs Complexity: Spark adds operational complexity but is justified if data volume or transformation complexity grows; for very small datasets, a lightweight runner (Pandas) could be used for prototyping.
- Local development parity: Docker + MinIO approximates production S3 and cluster behavior without external dependencies.
- Observability & data quality: Medallion layering plus explicit validation keeps errors isolated and auditable.

## Getting started
Start the stack:
```bash
docker-compose up -d
```

## Next steps and recommendations
- Add basic metrics/monitoring (pipeline durations, error counts).
- Handle a duplicated values following some business rules

