from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from dotenv import load_dotenv
import os


def ingest_to_s3_bronze():
    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    # Configure Spark with S3A (S3 connectivity for Hadoop)
    spark = (
        SparkSession.builder.appName("Togo_S3_Bronze_Ingestion")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        # Override any bad timeout values with plain integers
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Override all duration configs with plain integers
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
            .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
            .config("spark.hadoop.fs.s3a.retry.interval", "500")
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
            .config("spark.hadoop.fs.s3a.retry.throttle.interval", "500")
        .getOrCreate()
    )

    # 1. READ from Landing Zone (JSON)
    source_path = "s3a://togo-public-srv/raw/demandes_services_publics_togo.json"
    print(f"Reading raw data from {source_path}...")

    df_raw = spark.read.option("multiLine", "true").json(source_path)

    bronze_output_path = "s3a://togo-public-srv/bronze/"

    df_bronze = df_raw.withColumn(
        "ingestion_timestamp", current_timestamp()
    ).withColumn("source_file", input_file_name())
    print(f"Writing raw Parquet to {bronze_output_path}...")
    df_bronze.write.mode("overwrite").parquet(bronze_output_path)

    print("✅ Bronze Ingestion Complete.")
    spark.stop()


if __name__ == "__main__":
    ingest_to_s3_bronze()
