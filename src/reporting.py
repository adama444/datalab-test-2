from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc
from dotenv import load_dotenv
import os


def run_gold_to_postgres():
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

    # 1. Read Cleaned Data from Silver
    silver_path = "s3a://togo-public-srv/silver/"
    df_silver = spark.read.parquet(silver_path)

    # 4. Aggregation: Commune Priority Metrics
    commune_metrics = (
        df_silver.groupBy("standard_commune", "standard_service")
        .agg(count("standard_id").alias("request_count"))
        .orderBy(desc("request_count"))
    )

    gold_path = "s3a://togo-public-srv/gold/"
    
    df_commune_stats = commune_metrics.withColumnRenamed("standard_commune", "commune")
    df_commune_stats = df_commune_stats.withColumnRenamed("standard_service", "service")
    df_commune_stats = df_commune_stats.withColumnRenamed("request_count", "total_requests")
    df_commune_stats.write \
        .mode("overwrite") \
        .partitionBy("commune") \
        .parquet(gold_path)

    print("--- Gold Layer successfully pushed to Database ---")
    spark.stop()


if __name__ == "__main__":
    run_gold_to_postgres()
