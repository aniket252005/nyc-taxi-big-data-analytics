# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Data Ingestion
# MAGIC
# MAGIC **Objective:** Load raw NYC Yellow Taxi Parquet data into Spark and
# MAGIC establish the Bronze layer of the data pipeline.
# MAGIC
# MAGIC **Environment:** Works locally (PySpark) *and* on Databricks.

# COMMAND ----------

import os
import sys
import logging

# Add project root to path so we can import src modules
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Initialize Spark Session

# COMMAND ----------

# Detect environment: Databricks provides a pre-built `spark` variable
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

if not IS_DATABRICKS:
    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Ingestion")
        .master("local[*]")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    logger.info("Local SparkSession created.")
else:
    logger.info("Running on Databricks — using existing SparkSession.")

spark.sparkContext.setLogLevel("WARN")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Download sample data (local only)

# COMMAND ----------

if not IS_DATABRICKS:
    from src.ingestion import download_taxi_data, download_zone_lookup

    parquet_path = download_taxi_data(year=2024, month=1)
    zone_path = download_zone_lookup()
    logger.info("Sample data ready at: %s", parquet_path)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Read Parquet into Spark DataFrame (Bronze layer)

# COMMAND ----------

if IS_DATABRICKS:
    # --- Databricks: read from DBFS ---
    PARQUET_PATH = "dbfs:/FileStore/tables/yellow_tripdata_*.parquet"
    ZONE_PATH = "dbfs:/FileStore/tables/taxi_zone_lookup.csv"
else:
    # --- Local: read downloaded files ---
    PARQUET_PATH = os.path.join("data", "*.parquet")
    ZONE_PATH = os.path.join("data", "taxi_zone_lookup.csv")

df_bronze = spark.read.parquet(PARQUET_PATH)

logger.info("Bronze DataFrame loaded.")
logger.info("  Records : %d", df_bronze.count())
logger.info("  Columns : %d", len(df_bronze.columns))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Inspect Schema

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Quick Sample

# COMMAND ----------

df_bronze.show(10, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Load Zone Lookup Table

# COMMAND ----------

df_zones = spark.read.csv(ZONE_PATH, header=True, inferSchema=True)
df_zones.show(10)
logger.info("Zone lookup loaded: %d rows", df_zones.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Save Bronze Layer (Databricks only)
# MAGIC
# MAGIC On Databricks, persist the raw data as a managed Delta table.

# COMMAND ----------

if IS_DATABRICKS:
    df_bronze.write.format("delta").mode("overwrite").saveAsTable("nyc_taxi_bronze")
    logger.info("Bronze Delta table saved: nyc_taxi_bronze")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Ingestion Complete
# MAGIC
# MAGIC The raw (Bronze) data is now loaded and ready for cleaning in
# MAGIC **02_data_quality_and_cleaning.py**.
