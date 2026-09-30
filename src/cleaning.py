"""
src/cleaning.py — Data Quality & Cleaning Module
==================================================
Validates, cleans, and produces a data-quality report for NYC Yellow Taxi
trip data using PySpark.  All thresholds are configurable.
"""

import logging
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType,
    TimestampType, StringType, LongType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Expected schema (used for validation, not enforcement)
# ---------------------------------------------------------------------------
EXPECTED_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID",
    "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge", "Airport_fee",
]

# ---------------------------------------------------------------------------
# Validation thresholds
# ---------------------------------------------------------------------------
MIN_PASSENGERS = 1
MAX_PASSENGERS = 9
MIN_TRIP_DISTANCE = 0.1       # miles
MAX_TRIP_DISTANCE = 200.0     # miles
MIN_TRIP_DURATION_SEC = 60    # 1 minute
MAX_TRIP_DURATION_SEC = 18000 # 5 hours
MIN_FARE = 2.50               # USD
MAX_FARE = 500.0              # USD
MIN_TIP = 0.0
MAX_TIP = 200.0
MIN_YEAR = 2019
MAX_YEAR = 2026


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------
def validate_schema(df: DataFrame) -> dict:
    """Check that all expected columns are present. Return a report dict."""
    actual = set(df.columns)
    expected = set(EXPECTED_COLUMNS)
    missing = expected - actual
    extra = actual - expected
    report = {
        "expected_columns": len(expected),
        "actual_columns": len(actual),
        "missing_columns": sorted(missing),
        "extra_columns": sorted(extra),
        "schema_valid": len(missing) == 0,
    }
    if missing:
        logger.warning("Missing columns: %s", missing)
    return report


# ---------------------------------------------------------------------------
# Null detection
# ---------------------------------------------------------------------------
def count_nulls(df: DataFrame) -> DataFrame:
    """Return a single-row DataFrame with null counts per column."""
    null_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]
    return df.select(null_exprs)


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------
def count_duplicates(df: DataFrame) -> int:
    """Count fully-duplicate rows."""
    total = df.count()
    distinct = df.distinct().count()
    dupes = total - distinct
    logger.info("Duplicates detected: %d / %d", dupes, total)
    return dupes


# ---------------------------------------------------------------------------
# Row-level validation flags
# ---------------------------------------------------------------------------
def add_validation_flags(df: DataFrame) -> DataFrame:
    """
    Add boolean columns flagging invalid values.
    Returns the DataFrame with extra `_invalid_*` columns.
    """
    # Compute trip duration in seconds for validation
    df = df.withColumn(
        "_trip_duration_sec",
        F.unix_timestamp("tpep_dropoff_datetime")
        - F.unix_timestamp("tpep_pickup_datetime"),
    )

    df = (
        df
        # Invalid passenger count
        .withColumn(
            "_invalid_passengers",
            (F.col("passenger_count").isNull())
            | (F.col("passenger_count") < MIN_PASSENGERS)
            | (F.col("passenger_count") > MAX_PASSENGERS),
        )
        # Invalid trip distance
        .withColumn(
            "_invalid_distance",
            (F.col("trip_distance").isNull())
            | (F.col("trip_distance") < MIN_TRIP_DISTANCE)
            | (F.col("trip_distance") > MAX_TRIP_DISTANCE),
        )
        # Invalid trip duration
        .withColumn(
            "_invalid_duration",
            (F.col("_trip_duration_sec").isNull())
            | (F.col("_trip_duration_sec") < MIN_TRIP_DURATION_SEC)
            | (F.col("_trip_duration_sec") > MAX_TRIP_DURATION_SEC),
        )
        # Invalid fare amount
        .withColumn(
            "_invalid_fare",
            (F.col("fare_amount").isNull())
            | (F.col("fare_amount") < MIN_FARE)
            | (F.col("fare_amount") > MAX_FARE),
        )
        # Invalid tip amount
        .withColumn(
            "_invalid_tip",
            (F.col("tip_amount").isNull())
            | (F.col("tip_amount") < MIN_TIP)
            | (F.col("tip_amount") > MAX_TIP),
        )
        # Invalid timestamps (outside reasonable date range)
        .withColumn(
            "_invalid_pickup_time",
            (F.col("tpep_pickup_datetime").isNull())
            | (F.year("tpep_pickup_datetime") < MIN_YEAR)
            | (F.year("tpep_pickup_datetime") > MAX_YEAR),
        )
        .withColumn(
            "_invalid_dropoff_time",
            (F.col("tpep_dropoff_datetime").isNull())
            | (F.year("tpep_dropoff_datetime") < MIN_YEAR)
            | (F.year("tpep_dropoff_datetime") > MAX_YEAR),
        )
        # Negative total amount
        .withColumn(
            "_invalid_total",
            (F.col("total_amount").isNull()) | (F.col("total_amount") < 0),
        )
        # Combined flag: row is invalid if ANY flag is True
        .withColumn(
            "_is_invalid",
            F.col("_invalid_passengers")
            | F.col("_invalid_distance")
            | F.col("_invalid_duration")
            | F.col("_invalid_fare")
            | F.col("_invalid_tip")
            | F.col("_invalid_pickup_time")
            | F.col("_invalid_dropoff_time")
            | F.col("_invalid_total"),
        )
    )
    return df


# ---------------------------------------------------------------------------
# Build data-quality summary
# ---------------------------------------------------------------------------
def build_quality_summary(df: DataFrame) -> dict:
    """
    Run all quality checks and return a summary dict.
    Expects *df* to already have validation flags (call add_validation_flags first).
    """
    total = df.count()

    # Null rows (any column null among key fields)
    key_cols = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "fare_amount",
        "total_amount", "PULocationID", "DOLocationID",
    ]
    null_condition = F.lit(False)
    for c in key_cols:
        null_condition = null_condition | F.col(c).isNull()
    null_rows = df.filter(null_condition).count()

    # Duplicate rows
    duplicate_rows = count_duplicates(df)

    # Invalid rows (from validation flags)
    invalid_rows = df.filter(F.col("_is_invalid") == True).count()  # noqa: E712

    # Breakdown by flag
    flag_cols = [c for c in df.columns if c.startswith("_invalid_")]
    breakdown = {}
    for flag in flag_cols:
        cnt = df.filter(F.col(flag) == True).count()  # noqa: E712
        breakdown[flag.replace("_invalid_", "")] = cnt

    valid_rows = total - invalid_rows
    pct_removed = round((invalid_rows / total) * 100, 2) if total > 0 else 0.0

    summary = {
        "total_records": total,
        "null_records": null_rows,
        "duplicate_records": duplicate_rows,
        "invalid_records": invalid_rows,
        "valid_records": valid_rows,
        "pct_removed": pct_removed,
        "breakdown": breakdown,
    }

    logger.info("=== Data Quality Summary ===")
    for k, v in summary.items():
        if k != "breakdown":
            logger.info("  %-22s : %s", k, v)
    for k, v in breakdown.items():
        logger.info("    invalid_%-14s : %d", k, v)

    return summary


# ---------------------------------------------------------------------------
# Clean: remove invalid rows and drop helper columns
# ---------------------------------------------------------------------------
def clean_dataframe(df: DataFrame) -> DataFrame:
    """Remove invalid rows and drop internal validation columns."""
    df_flagged = add_validation_flags(df)
    df_clean = df_flagged.filter(F.col("_is_invalid") == False)  # noqa: E712

    # Drop all internal helper columns
    internal_cols = [c for c in df_clean.columns if c.startswith("_")]
    df_clean = df_clean.drop(*internal_cols)

    logger.info("Cleaning complete. Rows: %d → %d", df.count(), df_clean.count())
    return df_clean
