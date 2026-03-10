from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os
import sys

# On ajoute le dossier scripts au path pour pouvoir importer ingest_to_minio
sys.path.append('/opt/airflow/scripts')
from upload_to_minio import ingest_to_minio

default_args = {
    'owner': 'adama',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_failed': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'togo_public_services_pipeline',
    default_args=default_args,
    description='Pipeline End-to-End: Ingestion MinIO et Transformation Spark',
    schedule_interval='0 1 * * *',  # Tous les jours à 1h du matin
    catchup=False
) as dag:
    start = EmptyOperator(task_id='start_pipeline')

    # 1. Tâche d'ingestion (Landing -> Bronze)
    ingest_task = PythonOperator(
        task_id='ingest_json_to_minio',
        python_callable=ingest_to_minio,
        op_kwargs={
            'file_path': '/opt/airflow/data/demandes_services_publics.json',
            'bucket_name': os.getenv('MINIO_BUCKET_RAW', 'raw'),
            'object_name': 'demandes_togo_raw.json'
        }
    )

    # 2. Tâche de transformation (Bronze -> Silver)
    # Note: SparkSubmitOperator va envoyer le script au spark-master
    transform_task = SparkSubmitOperator(
        task_id='spark_transform_silver',
        application='/opt/airflow/src/transformation.py',
        application_args=["demandes_togo_raw.json", "{{ ds_nodash }}"],
        conn_id='spark_default',
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0",
        conf={
            "spark.hadoop.fs.s3a.endpoint": os.getenv('MINIO_ENDPOINT'),
            "spark.hadoop.fs.s3a.access.key": os.getenv('MINIO_ROOT_USER'),
            "spark.hadoop.fs.s3a.secret.key": os.getenv('MINIO_ROOT_PASSWORD'),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        },
        verbose=True
    )
    
    end = EmptyOperator(task_id='end_pipeline')

    # Définition de l'ordre des tâches
    start >> ingest_task >> transform_task >> end