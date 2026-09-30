"""
src/transformations.py — Feature Engineering Module
=====================================================
Creates derived columns for analytical queries using native PySpark functions.
Avoids Pandas UDFs for scalability on large datasets.
"""

import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


def add_time_features(df: DataFrame) -> DataFrame:
    """Extract temporal features from pickup and drop-off timestamps."""
    df = (
        df
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day", F.dayofmonth("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))  # 1=Sun … 7=Sat
        .withColumn("pickup_day_name", F.date_format("tpep_pickup_datetime", "EEEE"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
    )
    logger.info("Added time features (hour, day, day_of_week, month, year).")
    return df


def add_trip_metrics(df: DataFrame) -> DataFrame:
    """Compute trip duration, speed, and fare-efficiency metrics."""
    df = (
        df
        # Trip duration in minutes
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            ) / 60.0,
        )
        # Average speed (mph) — guard against zero-duration trips
        .withColumn(
            "average_speed",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0),
            ).otherwise(0.0),
        )
        # Fare per mile — guard against zero-distance trips
        .withColumn(
            "fare_per_mile",
            F.when(
                F.col("trip_distance") > 0,
                F.col("fare_amount") / F.col("trip_distance"),
            ).otherwise(0.0),
        )
    )
    logger.info("Added trip metrics (duration, speed, fare_per_mile).")
    return df


def add_revenue_features(df: DataFrame) -> DataFrame:
    """Compute tip percentage and total revenue."""
    df = (
        df
        # Tip percentage relative to fare
        .withColumn(
            "tip_percentage",
            F.when(
                F.col("fare_amount") > 0,
                F.round((F.col("tip_amount") / F.col("fare_amount")) * 100, 2),
            ).otherwise(0.0),
        )
        # Total revenue (= total_amount; explicit alias for clarity in SQL)
        .withColumn("total_revenue", F.col("total_amount"))
    )
    logger.info("Added revenue features (tip_percentage, total_revenue).")
    return df


def add_day_type_features(df: DataFrame) -> DataFrame:
    """Flag weekend trips and peak-hour trips."""
    # dayofweek: 1=Sunday, 7=Saturday → weekend = {1, 7}
    df = (
        df
        .withColumn(
            "is_weekend",
            F.when(
                F.col("pickup_day_of_week").isin(1, 7), True
            ).otherwise(False),
        )
        # Peak hours: 7-10 AM and 4-8 PM (typical NYC commute windows)
        .withColumn(
            "is_peak_hour",
            F.when(
                (F.col("pickup_hour").between(7, 9))
                | (F.col("pickup_hour").between(16, 19)),
                True,
            ).otherwise(False),
        )
        # Friendly day-type label
        .withColumn(
            "day_type",
            F.when(F.col("is_weekend"), "Weekend").otherwise("Weekday"),
        )
    )
    logger.info("Added day-type features (is_weekend, is_peak_hour, day_type).")
    return df


# ---------------------------------------------------------------------------
# Master transformation pipeline
# ---------------------------------------------------------------------------
def apply_all_transformations(df: DataFrame) -> DataFrame:
    """Run every feature-engineering step in order."""
    logger.info("Starting feature engineering pipeline …")
    df = add_time_features(df)
    df = add_trip_metrics(df)
    df = add_revenue_features(df)
    df = add_day_type_features(df)
    logger.info(
        "Feature engineering complete. Final column count: %d", len(df.columns)
    )
    return df
