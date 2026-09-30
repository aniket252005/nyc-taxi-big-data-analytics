# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Spark Performance Optimization
# MAGIC
# MAGIC **Objective:** Demonstrate and benchmark Spark optimization techniques.
# MAGIC
# MAGIC > ⚠️ **Note:** Accurate timings require a Spark cluster (Databricks).
# MAGIC > On a local single-node setup, improvements may not be observable due
# MAGIC > to small data size.  Placeholders are clearly marked.

# COMMAND ----------

import os, sys, time, logging

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

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
        .appName("NYC-Taxi-Optimization")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.adaptive.enabled", "true")  # AQE
        .getOrCreate()
    )
spark.sparkContext.setLogLevel("WARN")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Load Data

# COMMAND ----------

if IS_DATABRICKS:
    df = spark.table("nyc_taxi_silver")
else:
    silver_path = os.path.join("data", "silver")
    if os.path.exists(silver_path):
        df = spark.read.parquet(silver_path)
    else:
        df = spark.read.parquet(os.path.join("data", "*.parquet"))

record_count = df.count()
logger.info("Loaded %d records", record_count)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Benchmarking Helper

# COMMAND ----------

def benchmark(name: str, func, *args, **kwargs):
    """Run *func*, measure wall-clock time, and return (result, seconds)."""
    spark.catalog.clearCache()  # ensure fair comparison
    start = time.time()
    result = func(*args, **kwargs)
    # Force materialization
    if hasattr(result, "count"):
        result.count()
    elif hasattr(result, "collect"):
        result.collect()
    elapsed = round(time.time() - start, 3)
    logger.info("⏱  %-40s : %.3f s", name, elapsed)
    return result, elapsed

benchmarks = []  # collect results for the summary table

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Optimization Technique 1 — Column Selection (Projection Pushdown)
# MAGIC
# MAGIC Reading fewer columns from Parquet is dramatically cheaper because
# MAGIC Parquet is columnar — unneeded columns are never read from disk.

# COMMAND ----------

# BAD: select all columns
_, t_all = benchmark(
    "Select ALL columns",
    lambda: df.groupBy("PULocationID").agg(F.count("*").alias("cnt")),
)

# GOOD: select only needed columns
_, t_proj = benchmark(
    "Select ONLY needed columns",
    lambda: df.select("PULocationID").groupBy("PULocationID").agg(F.count("*").alias("cnt")),
)

benchmarks.append(("Column Selection", t_all, t_proj))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Optimization Technique 2 — Predicate / Filter Pushdown
# MAGIC
# MAGIC Filtering early reduces the volume of data shuffled downstream.

# COMMAND ----------

# BAD: aggregate first, then filter
_, t_no_push = benchmark(
    "Aggregate THEN filter",
    lambda: (
        df.groupBy("PULocationID", "pickup_hour")
        .agg(F.count("*").alias("cnt"), F.sum("fare_amount").alias("rev"))
        .filter(F.col("pickup_hour").between(7, 9))
    ),
)

# GOOD: filter first, then aggregate
_, t_push = benchmark(
    "Filter THEN aggregate",
    lambda: (
        df.filter(F.col("pickup_hour").between(7, 9))
        .groupBy("PULocationID", "pickup_hour")
        .agg(F.count("*").alias("cnt"), F.sum("fare_amount").alias("rev"))
    ),
)

benchmarks.append(("Filter Pushdown", t_no_push, t_push))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Optimization Technique 3 — Broadcast Join
# MAGIC
# MAGIC When one side of a join is small (e.g., zone lookup), broadcasting
# MAGIC avoids an expensive shuffle join.

# COMMAND ----------

# Create a small lookup DataFrame (simulating zone lookup)
zone_data = [(i, f"Zone_{i}") for i in range(1, 266)]
df_zones = spark.createDataFrame(zone_data, ["LocationID", "ZoneName"])

# BAD: default sort-merge join (no hint)
_, t_smj = benchmark(
    "Sort-Merge Join (default)",
    lambda: df.join(df_zones, df.PULocationID == df_zones.LocationID, "left"),
)

# GOOD: broadcast join
_, t_bcast = benchmark(
    "Broadcast Join",
    lambda: df.join(F.broadcast(df_zones), df.PULocationID == df_zones.LocationID, "left"),
)

benchmarks.append(("Broadcast Join", t_smj, t_bcast))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Optimization Technique 4 — Repartition vs Coalesce
# MAGIC
# MAGIC * `repartition(N)` — full shuffle, use when increasing partitions.
# MAGIC * `coalesce(N)` — no shuffle, use when *decreasing* partitions.

# COMMAND ----------

logger.info("Current partitions: %d", df.rdd.getNumPartitions())

# BAD: repartition to reduce partitions (triggers a full shuffle)
_, t_repart = benchmark(
    "repartition(4) to reduce",
    lambda: df.repartition(4),
)

# GOOD: coalesce to reduce partitions (no shuffle)
_, t_coal = benchmark(
    "coalesce(4) to reduce",
    lambda: df.coalesce(4),
)

benchmarks.append(("Repartition vs Coalesce", t_repart, t_coal))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Optimization Technique 5 — Caching (When Useful)
# MAGIC
# MAGIC Caching is beneficial when the same DataFrame is reused multiple times.

# COMMAND ----------

# First access — uncached
_, t_uncached_1 = benchmark("Uncached query 1", lambda: df.filter(F.col("fare_amount") > 10).agg(F.avg("fare_amount")))
_, t_uncached_2 = benchmark("Uncached query 2", lambda: df.filter(F.col("fare_amount") > 10).agg(F.sum("total_amount")))

# Cache the filtered DataFrame and reuse
df_cached = df.filter(F.col("fare_amount") > 10).cache()
df_cached.count()  # materialize cache

_, t_cached_1 = benchmark("Cached query 1", lambda: df_cached.agg(F.avg("fare_amount")))
_, t_cached_2 = benchmark("Cached query 2", lambda: df_cached.agg(F.sum("total_amount")))

df_cached.unpersist()

benchmarks.append(("Caching (2nd reuse)", t_uncached_2, t_cached_2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Optimization Technique 6 — Parquet Partitioning
# MAGIC
# MAGIC Writing data partitioned by a frequently-filtered column allows
# MAGIC Spark to skip entire partitions during reads (partition pruning).

# COMMAND ----------

# Write partitioned Parquet (demonstration — not benchmarked on read here)
if not IS_DATABRICKS:
    part_path = os.path.join("data", "gold", "partitioned_by_hour")
    (
        df.select("PULocationID", "pickup_hour", "fare_amount", "total_amount")
        .write.mode("overwrite")
        .partitionBy("pickup_hour")
        .parquet(part_path)
    )
    logger.info("Partitioned Parquet written to: %s", part_path)

    # Read back with partition filter
    _, t_part_read = benchmark(
        "Partitioned read (hour=8)",
        lambda: spark.read.parquet(part_path).filter(F.col("pickup_hour") == 8),
    )
    logger.info("Partitioned read time: %.3f s", t_part_read)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Benchmark Summary Table

# COMMAND ----------

print("\n" + "=" * 72)
print(f"{'Query':<30} {'Before (s)':>12} {'After (s)':>12} {'Improvement':>14}")
print("=" * 72)

for name, before, after in benchmarks:
    if before > 0:
        improvement = round(((before - after) / before) * 100, 1)
        imp_str = f"{improvement}%"
    else:
        imp_str = "N/A"
    print(f"{name:<30} {before:>12.3f} {after:>12.3f} {imp_str:>14}")

print("=" * 72)
print()
print("NOTE: On a local single-node setup with small data, improvements")
print("may be marginal or negative due to overhead.  Run on Databricks")
print("with the full dataset for meaningful benchmarks.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Optimization Notebook Complete
# MAGIC
# MAGIC For production-grade benchmarking:
# MAGIC 1. Upload to Databricks.
# MAGIC 2. Attach a multi-node cluster (e.g., 4 × Standard_DS3_v2).
# MAGIC 3. Load the full dataset (12+ months).
# MAGIC 4. Re-run this notebook — the summary table will fill with
# MAGIC    real, reproducible timings.
