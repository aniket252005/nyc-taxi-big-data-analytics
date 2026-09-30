# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Data Quality & Cleaning
# MAGIC
# MAGIC **Objective:** Validate raw data, detect quality issues, clean the
# MAGIC dataset, and produce the **Silver** layer.
# MAGIC
# MAGIC All metrics are computed from the actual dataset — nothing is fabricated.

# COMMAND ----------

import os, sys, logging, json

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from src.cleaning import (
    validate_schema,
    count_nulls,
    count_duplicates,
    add_validation_flags,
    build_quality_summary,
    clean_dataframe,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
logger = logging.getLogger(__name__)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Initialize Spark

# COMMAND ----------

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

if not IS_DATABRICKS:
    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Cleaning")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
spark.sparkContext.setLogLevel("WARN")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Load Bronze Data

# COMMAND ----------

if IS_DATABRICKS:
    df_bronze = spark.table("nyc_taxi_bronze")
else:
    df_bronze = spark.read.parquet(os.path.join("data", "*.parquet"))

total_records = df_bronze.count()
logger.info("Bronze records loaded: %d", total_records)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Schema Validation

# COMMAND ----------

schema_report = validate_schema(df_bronze)
print(json.dumps(schema_report, indent=2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Null Analysis

# COMMAND ----------

null_counts = count_nulls(df_bronze)
null_counts.show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Duplicate Detection

# COMMAND ----------

dup_count = count_duplicates(df_bronze)
print(f"Duplicate rows: {dup_count:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Row-level Validation

# COMMAND ----------

df_flagged = add_validation_flags(df_bronze)

# Show breakdown of invalid-row flags
flag_cols = [c for c in df_flagged.columns if c.startswith("_invalid_")]
for flag in flag_cols:
    cnt = df_flagged.filter(F.col(flag) == True).count()  # noqa: E712
    print(f"  {flag}: {cnt:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Data Quality Summary

# COMMAND ----------

quality_summary = build_quality_summary(df_flagged)

print("\n========== DATA QUALITY SUMMARY ==========")
print(f"  Total records       : {quality_summary['total_records']:,}")
print(f"  Null records        : {quality_summary['null_records']:,}")
print(f"  Duplicate records   : {quality_summary['duplicate_records']:,}")
print(f"  Invalid records     : {quality_summary['invalid_records']:,}")
print(f"  Valid records       : {quality_summary['valid_records']:,}")
print(f"  % Removed           : {quality_summary['pct_removed']}%")
print("==========================================\n")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Clean the DataFrame

# COMMAND ----------

df_silver = clean_dataframe(df_bronze)
silver_count = df_silver.count()
logger.info("Silver DataFrame: %d records", silver_count)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Verify Silver Data

# COMMAND ----------

df_silver.describe().show()
df_silver.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Save Silver Layer

# COMMAND ----------

if IS_DATABRICKS:
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("PULocationID")
        .saveAsTable("nyc_taxi_silver")
    )
    logger.info("Silver Delta table saved: nyc_taxi_silver")
else:
    silver_path = os.path.join("data", "silver")
    (
        df_silver.write
        .mode("overwrite")
        .partitionBy("PULocationID")
        .parquet(silver_path)
    )
    logger.info("Silver Parquet saved to: %s", silver_path)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Cleaning Complete
# MAGIC
# MAGIC Silver layer is ready for feature engineering in
# MAGIC **03_eda_analysis.py**.
