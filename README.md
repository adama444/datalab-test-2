# Exercise 2: Data Pipeline with NoSQL Data Source
## 1. Project Overview & Context
The project aim to automate the ingestion and normalization of heterogeneous Togolese Public Service Requests (citizen reports, administrative requests, complaints, etc.) from a NoSQL source (MongoDB) into an analytics-ready format.

## 2. Architecture & Tech Stack
- **Ingestion**: I will use Python (PyMongo) for seeding the MongoDB with provided json file. And will use Spark (PySpark) for its flexibility with missing fields.
- **Orchestration**: I will use Apache Airflow to manage the workflow, retries, and task dependencies. With Airflow we can design the workflow across tasks
- **Processing**: I will use Apache Spark to handle distributed transformations and schema evolution at scale.
- **Data Quality**: Great Expectations – To ensure "Silver" data adheres to business rules before reaching stakeholders.
- **Storage**: Medallion Architecture (Bronze/Silver/Gold) stored in Parquet.

## 3. Data Pipeline Logic
1. **Stage 1: Bronze (Raw)**
    - I will use the data from MongoDB collection and do extraction and storage as Parquet.
    - I assume we treat the NoSQL source as "The Truth," capturing all nested fields and inconsistent keys.
    - Data is stored in Parquet format. This allows for schema evolution support and provides significant compression.

2. **Stage 2: Silver (Cleaned & Normalized)**
    - Schema Evolution: Explain how you handle new fields. If a document arrives with a new commune field that didn't exist yesterday, Spark's schema inference will adapt or store it in a catch-all metadata column.

    - Normalization:
        - Dates: Standardizing DD/MM/YYYY, ISO8601, and Timestamp into a single UTC format.
        - Geospatial: Extracting lat/long from various formats (nested vs. string).
        - Deduplication: Removing duplicate request_id entries.

3. **Stage 3: Gold (Analytical)**
    - Action: Aggregations for the Togolese administration.
    - Example KPIs:
        - Report Volume by Region: (Maritime, Plateaux, Centrale, Kara, Savanes).
        - Service Efficiency: Average resolution time for "Civil Status" vs "Education" requests.

## 4. Data Quality & Robustness
Explain how you manage "Imperfect Data":
- Validation Rules: "Every record must have a valid service_type."
- Alerting: Logic for handling records that fail validation (The "Dead Letter Queue" concept).

## 5. Deployment & Execution
- **Prerequisites**: 
    - Docker
    - Docker Compose
- **Setup**:
```bash
    docker-compose up -d
    docker exec -it python scripts/seed_mongo.py
```
- Trigger the Airflow DAG by accessing the Airflow UI at http://localhost:8090

## 6. Future Improvements
- CI/CD: Automating unit tests for Spark transformations.
- Monitoring: Integrating Prometheus/Grafana to track pipeline latency.
