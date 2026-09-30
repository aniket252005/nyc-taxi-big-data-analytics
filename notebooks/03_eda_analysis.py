# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — EDA & Feature Engineering
# MAGIC
# MAGIC **Objective:** Apply feature engineering, run exploratory analysis,
# MAGIC compute key insights, and produce the **Gold** layer.

# COMMAND ----------

import os, sys, logging, json

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from src.transformations import apply_all_transformations
from src.analysis import (
    hourly_demand,
    daily_demand,
    peak_vs_nonpeak,
    weekend_vs_weekday,
    top_pickup_zones,
    top_dropoff_zones,
    revenue_by_zone,
    avg_fare_by_zone,
    revenue_by_hour,
    payment_type_distribution,
    trip_distance_distribution,
    rank_zones_by_revenue,
    generate_key_insights,
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
        .appName("NYC-Taxi-EDA")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
spark.sparkContext.setLogLevel("WARN")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Load Silver Data

# COMMAND ----------

if IS_DATABRICKS:
    df_silver = spark.table("nyc_taxi_silver")
else:
    silver_path = os.path.join("data", "silver")
    if os.path.exists(silver_path):
        df_silver = spark.read.parquet(silver_path)
    else:
        # Fallback: read raw and clean inline
        from src.cleaning import clean_dataframe
        df_raw = spark.read.parquet(os.path.join("data", "*.parquet"))
        df_silver = clean_dataframe(df_raw)

logger.info("Silver records: %d", df_silver.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Feature Engineering

# COMMAND ----------

df_enriched = apply_all_transformations(df_silver)
df_enriched.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Register as Spark SQL Temp View

# COMMAND ----------

df_enriched.createOrReplaceTempView("taxi_trips")
logger.info("Temp view 'taxi_trips' registered for Spark SQL queries.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Exploratory Analysis

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.1 Hourly Demand

# COMMAND ----------

hourly = hourly_demand(df_enriched)
hourly.show(24)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.2 Daily Demand

# COMMAND ----------

daily = daily_demand(df_enriched)
daily.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.3 Peak vs Non-Peak

# COMMAND ----------

peak = peak_vs_nonpeak(df_enriched)
peak.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.4 Weekend vs Weekday

# COMMAND ----------

wk = weekend_vs_weekday(df_enriched)
wk.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.5 Top 10 Pickup Zones

# COMMAND ----------

top_pu = top_pickup_zones(df_enriched, n=10)
top_pu.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.6 Top 10 Drop-off Zones

# COMMAND ----------

top_do = top_dropoff_zones(df_enriched, n=10)
top_do.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.7 Revenue by Hour

# COMMAND ----------

rev_hr = revenue_by_hour(df_enriched)
rev_hr.show(24)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.8 Revenue by Zone

# COMMAND ----------

rev_zone = revenue_by_zone(df_enriched, n=10)
rev_zone.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.9 Payment Type Distribution

# COMMAND ----------

pay = payment_type_distribution(df_enriched)
pay.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.10 Trip Distance Distribution

# COMMAND ----------

dist = trip_distance_distribution(df_enriched)
dist.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.11 Zone Revenue Ranking (Window Function)

# COMMAND ----------

ranked = rank_zones_by_revenue(df_enriched)
ranked.show(15)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Key Insights

# COMMAND ----------

insights = generate_key_insights(df_enriched)
print("\n" + json.dumps(insights, indent=2, default=str) + "\n")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Save Gold Layer
# MAGIC
# MAGIC Save aggregated tables for downstream consumption (Power BI, dashboards).

# COMMAND ----------

gold_tables = {
    "gold_hourly_demand": hourly,
    "gold_daily_demand": daily,
    "gold_peak_demand": peak,
    "gold_weekend_weekday": wk,
    "gold_top_pickup_zones": top_pu,
    "gold_top_dropoff_zones": top_do,
    "gold_revenue_by_hour": rev_hr,
    "gold_revenue_by_zone": rev_zone,
    "gold_payment_distribution": pay,
    "gold_distance_distribution": dist,
    "gold_zone_ranking": ranked,
}

if IS_DATABRICKS:
    for table_name, table_df in gold_tables.items():
        table_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        logger.info("Gold table saved: %s", table_name)
else:
    gold_dir = os.path.join("data", "gold")
    for table_name, table_df in gold_tables.items():
        path = os.path.join(gold_dir, table_name)
        table_df.write.mode("overwrite").parquet(path)
        logger.info("Gold Parquet saved: %s", path)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ EDA & Feature Engineering Complete
# MAGIC
# MAGIC Gold aggregated tables are ready.  
# MAGIC Next: run **04_spark_sql_analysis.sql** for Spark SQL demonstrations.
