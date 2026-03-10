#!/bin/bash
set -e

echo "Running Airflow DB migration..."
airflow db migrate

echo "Creating admin user..."
airflow users create \
  --username "$AIRFLOW_ADMIN_USERNAME" \
  --password "$AIRFLOW_ADMIN_PASSWORD" \
  --firstname Admin \
  --lastname Datalab \
  --role Admin \
  --email "$AIRFLOW_ADMIN_EMAIL" || echo "Admin user already exists"

echo "Creating MinIO connection..."
airflow connections add minio_default \
  --conn-type aws \
  --conn-login "$MINIO_ROOT_USER" \
  --conn-password "$MINIO_ROOT_PASSWORD" \
  --conn-extra "{\"endpoint_url\": \"${MINIO_ENDPOINT}\"}" || echo "MinIO connection exists"

echo "Creating Postgres connection..."
airflow connections add postgres_datalab \
  --conn-type postgres \
  --conn-host "$POSTGRES_HOST" \
  --conn-login "$POSTGRES_USER" \
  --conn-password "$POSTGRES_PASSWORD" \
  --conn-port "$POSTGRES_PORT" \
  --conn-schema "$POSTGRES_DB" || echo "PostgreSQL connection exists"

echo "Creating Spark connection..."
airflow connections add spark_default \
  --conn-type spark \
  --conn-host "spark://spark-master" \
  --conn-port 7077 || echo "Spark connection already exists"

echo "Airflow initialization completed!"