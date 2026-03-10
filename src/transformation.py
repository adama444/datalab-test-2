from datetime import datetime
import json
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    lower,
    regexp_replace,
    try_to_timestamp,
    from_unixtime,
    lit,
    when,
    split,
    trim,
)
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION LOGGING ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CATALOG_PATH = os.path.join(SCRIPT_DIR, "mapping_catalog.json")


def get_spark_session():
    return (
        SparkSession.builder.appName("Togo-Public-Services-Silver")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )


def resolve_column(df, field):
    """Gère les champs imbriqués (struct) et les champs plats."""
    if "." in field:
        parent = field.split(".")[0]
        if parent in df.columns:
            parent_type = dict(df.dtypes).get(parent, "")
            if parent_type.startswith("struct"):
                return col(field)
        return None  # parent est string ou absent → on ignore
    return col(field) if field in df.columns else None


def apply_geo_parsing(df, geo_cfg):
    geo_col = None
    for field in geo_cfg["source_fields"]:
        c = resolve_column(df, field)
        if c is not None:
            geo_col = c
            logging.info(f"Champ géographique détecté : '{field}'")
            break

    if geo_col is None:
        logging.warning(
            "Aucun champ géographique trouvé. latitude/longitude seront NULL."
        )
        df = df.withColumn("latitude", lit(None).cast("double"))
        df = df.withColumn("longitude", lit(None).cast("double"))
        return df

    geo_clean = trim(regexp_replace(geo_col.cast("string"), r"\s*,\s*", ","))

    df = df.withColumn(
        "latitude",
        when(
            geo_clean.contains(","),
            split(geo_clean, ",").getItem(0).cast("double"),
        ).otherwise(lit(None).cast("double")),
    )
    df = df.withColumn(
        "longitude",
        when(
            geo_clean.contains(","),
            split(geo_clean, ",").getItem(1).cast("double"),
        ).otherwise(lit(None).cast("double")),
    )

    df = df.withColumn(
        "latitude",
        when(col("latitude").between(-90, 90), col("latitude")).otherwise(
            lit(None).cast("double")
        ),
    )
    df = df.withColumn(
        "longitude",
        when(col("longitude").between(-180, 180), col("longitude")).otherwise(
            lit(None).cast("double")
        ),
    )

    return df


def run_silver_pipeline(filename="demandes_togo_raw.json", execution_date=None):
    current_date = execution_date or datetime.now().strftime("%Y%m%d")
    spark = get_spark_session()

    # 1. Chargement du catalogue et des données
    with open(CATALOG_PATH, "r") as f:
        catalog = json.load(f)

    raw_path = f"s3a://{os.getenv('MINIO_BUCKET_RAW')}/{filename}"
    df = spark.read.option("multiline", "true").json(raw_path)
    df.printSchema()

    # 2. Normalisation des colonnes via le Catalogue
    for target_col, source_fields in catalog["mappings"].items():
        # On filtre les colonnes qui existent réellement dans le fichier JSON actuel
        valid_sources = [
            resolve_column(df, f)
            for f in source_fields
            if resolve_column(df, f) is not None
        ]
        if valid_sources:
            df = df.withColumn(target_col, coalesce(*valid_sources))
        else:
            df = df.withColumn(target_col, lit(None))

    # 3. Cas particulier : localisation (STRING) (ex: "Agbodrafo - Aného")
    if "localisation" in df.columns:
        loc_type = dict(df.dtypes).get("localisation", "")

        if loc_type == "string":
            df = df.withColumn(
                "standard_neighborhood",
                when(
                    col("standard_neighborhood").isNull()
                    & col("localisation").contains(" - "),
                    trim(split(col("localisation"), " - ").getItem(0)),
                ).otherwise(col("standard_neighborhood")),
            )
            df = df.withColumn(
                "standard_commune",
                when(
                    col("standard_commune").isNull()
                    & col("localisation").contains(" - "),
                    trim(split(col("localisation"), " - ").getItem(1)),
                ).otherwise(col("standard_commune")),
            )

    # 4. Normalisation des dates (Timestamp Unix vs String)
    date_cfg = catalog["date_config"]
    date_expressions = []

    for f in date_cfg["source_fields"]:
        c = resolve_column(df, f)
        if c is not None:
            date_expressions.append(
                when(
                    c.cast("double").isNotNull(),
                    from_unixtime(c.cast("double")).cast("timestamp"),
                )
            )
            for fmt in date_cfg["formats"]:
                date_expressions.append(try_to_timestamp(c.cast("string"), lit(fmt)))

    df = df.withColumn("event_date", coalesce(*date_expressions))

    # 5. Parsing du champ géographique (lat,lon)
    geo_cfg = catalog.get("geo_config", {})
    if geo_cfg:
        df = apply_geo_parsing(df, geo_cfg)
    else:
        logging.warning(
            "Aucune section 'geo_config' dans le catalogue. Champs geo ignorés."
        )
        df = df.withColumn("latitude", lit(None).cast("double"))
        df = df.withColumn("longitude", lit(None).cast("double"))

    # 5. Nettoyage final
    final_df = df.select(
        col("standard_id").alias("request_id"),
        lower(col("standard_service")).alias("category"),
        lower(col("standard_status")).alias("status"),
        col("standard_commune").alias("commune"),
        col("standard_neighborhood").alias("neighborhood"),
        col("event_date").alias("request_timestamp"),
        col("latitude"),
        col("longitude"),
    ).filter(col("request_id").isNotNull())

    # 6. Écriture vers MinIO Silver (Format Parquet)
    silver_path = f"s3a://{os.getenv('MINIO_BUCKET_CLEANED')}/cleaned_{filename.split('.')[0]}_{current_date}"
    final_df.write.mode("overwrite").parquet(silver_path)

    logging.info(f"Pipeline Silver terminé. Données écrites dans {silver_path}")
    
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB")
    
    pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    
    logging.info(f"Démarrage de l'exportation vers PostgreSQL : {pg_url}")

    try:
        final_df.write \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", os.getenv("SILVER_SCHEMA") + "." + os.getenv("SILVER_TABLE")) \
            .option("user", os.getenv("POSTGRES_USER")) \
            .option("password", os.getenv("POSTGRES_PASSWORD")) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logging.info(f"✅ Données chargées avec succès dans la table '{os.getenv('SILVER_SCHEMA')}.{os.getenv('SILVER_TABLE')}' !")
        
    except Exception as e:
        logging.error(f"❌ Erreur lors de l'écriture dans PostgreSQL : {e}")
        raise
    
    spark.stop()


if __name__ == "__main__":
    run_silver_pipeline()
