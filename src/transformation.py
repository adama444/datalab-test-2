import json
import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lower, try_to_timestamp, from_unixtime, lit, when
from dotenv import load_dotenv
from pyspark.sql.functions import get_json_object

# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=f"{LOG_DIR}/pipeline_{datetime.now().strftime('%Y%m%d')}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def resolve_column(df, field):
    parts = field.split(".")
    if len(parts) == 1:
        return col(field) if field in df.columns else None

    parent, subfield = parts[0], ".".join(parts[1:])
    parent_type = dict(df.dtypes).get(parent, "")

    if parent_type.startswith("struct"):
        return col(field)
    elif parent_type == "string":
        return get_json_object(col(parent), f"$.{subfield}")
    else:
        return None


def run_fully_dynamic_silver():
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

    with open("/app/config/mapping_catalog.json", "r") as f:
        catalog = json.load(f)

    # 2. Read from Bronze Zone
    bronze_path = "s3a://togo-public-srv/bronze/"
    logging.info(f"Reading from Bronze: {bronze_path}")
    df = spark.read.parquet(bronze_path)

    # --- 1. Dynamic Column Mapping ---
    for target_col, source_fields in catalog["mappings"].items():
        valid_cols = []
        for f in source_fields:
            parent = f.split(".")[0]
            if parent not in df.columns:
                continue
            resolved = resolve_column(df, f)
            if resolved is not None:
                valid_cols.append(resolved)

        if valid_cols:
            df = df.withColumn(target_col, lower(coalesce(*valid_cols)))
        else:
            df = df.withColumn(target_col, lit(None).cast("string"))

    # --- 2. Dynamic Date Normalization ---
    date_cfg = catalog["date_config"]
    date_expressions = []
    temp_cols = []  # track temp columns to drop later

    # Filter fields that actually exist in the dataframe
    for field in date_cfg["source_fields"]:
        # Check the parent column actually exists
        parent = field.split(".")[0]
        if parent not in df.columns:
            continue

        parent_type = dict(df.dtypes).get(parent, "")

        # Materialize the field into a real temp column first
        temp_col_name = f"_tmp_{field.replace('.', '_')}"
        temp_cols.append(temp_col_name)

        if "." not in field:
            df = df.withColumn(temp_col_name, col(field).cast("string"))
        elif parent_type.startswith("struct"):
            df = df.withColumn(temp_col_name, col(field).cast("string"))
        elif parent_type == "string":
            df = df.withColumn(
                temp_col_name,
                get_json_object(col(parent), f"$.{field.split('.', 1)[1]}"),
            )
        else:
            temp_cols.pop()  # didn't add it
            continue

        # A. Try parsing as Unix Timestamp
        # With this — only cast to double if value is numeric:
        date_expressions.append(
            when(
                col(temp_col_name).rlike(r"^\d+(\.\d+)?([eE]\d+)?$"),
                from_unixtime(col(temp_col_name).cast("double"))
            ).otherwise(lit(None).cast("timestamp"))
        )
        for fmt in date_cfg["formats"]:
            date_expressions.append(try_to_timestamp(col(temp_col_name), lit(fmt)))

    # Guard: only apply coalesce if we have expressions
    if date_expressions:
        df = df.withColumn("standard_date", coalesce(*date_expressions))
    else:
        logging.warning("No valid date fields found — standard_date will be null.")
        df = df.withColumn("standard_date", lit(None).cast("timestamp"))

    # Drop all temp columns
    df = df.drop(*temp_cols)

    # --- 3. Anomaly Detection & Logging ---
    # A record is an anomaly if it has no ID or no valid Date after all attempts
    df_valid = df.filter(
        col("standard_id").isNotNull() & col("standard_date").isNotNull()
    )
    df_anomalies = df.filter(
        col("standard_id").isNull() | col("standard_date").isNull()
    )

    anomaly_count = df_anomalies.count()
    if anomaly_count > 0:
        logging.warning(f"Detected {anomaly_count} anomalies.")
        sample = df_anomalies.limit(1).toJSON().first()
        logging.error(f"SCHEMA MISMATCH EXAMPLE: {sample}")

    # --- 4. Final Save ---
    final_cols = list(catalog["mappings"].keys()) + ["standard_date"]
    df_silver = df_valid.select(*final_cols)

    silver_path = "s3a://togo-public-srv/silver/"
    logging.info(f"Writing to Silver: {silver_path}")

    df_silver.write.mode("overwrite").parquet(silver_path)

    logging.info(
        f"✅ Silver Transformation Complete. Records processed: {df_silver.count()}"
    )
    spark.stop()


if __name__ == "__main__":
    run_fully_dynamic_silver()
