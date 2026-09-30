"""
tests/test_transformations.py — Unit Tests for Feature Engineering Module
"""

import pytest
pyspark = pytest.importorskip("pyspark")
from datetime import datetime
from src.transformations import (
    add_time_features,
    add_trip_metrics,
    add_revenue_features,
    add_day_type_features,
    apply_all_transformations,
)


def create_sample_df(spark):
    """Create a minimal DataFrame for testing transformations."""
    from pyspark.sql.types import (
        StructType, StructField, IntegerType, DoubleType, TimestampType,
    )

    schema = StructType([
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
    ])

    rows = [
        # Monday 8:30 AM (peak hour, weekday)
        (
            datetime(2024, 1, 15, 8, 30, 0),
            datetime(2024, 1, 15, 8, 50, 0),  # 20 min
            5.0,   # 5 miles
            20.0,  # fare
            4.0,   # tip (20%)
            25.0,  # total
        ),
        # Sunday 2:00 PM (off-peak, weekend)
        (
            datetime(2024, 1, 21, 14, 0, 0),
            datetime(2024, 1, 21, 14, 30, 0),  # 30 min
            10.0,  # 10 miles
            35.0,  # fare
            7.0,   # tip (20%)
            45.0,  # total
        ),
    ]
    return spark.createDataFrame(rows, schema=schema)


def test_time_features(spark):
    """Verify temporal column extractions."""
    df = create_sample_df(spark)
    df_transformed = add_time_features(df)

    rows = df_transformed.collect()
    assert rows[0]["pickup_hour"] == 8
    assert rows[0]["pickup_day"] == 15
    assert rows[0]["pickup_month"] == 1
    assert rows[0]["pickup_year"] == 2024


def test_trip_metrics(spark):
    """Verify duration, speed, and fare per mile computations."""
    df = create_sample_df(spark)
    df_transformed = add_trip_metrics(df)

    row1 = df_transformed.collect()[0]
    assert abs(row1["trip_duration_minutes"] - 20.0) < 0.01
    # 5 miles in 20 min (1/3 hr) -> 15.0 mph
    assert abs(row1["average_speed"] - 15.0) < 0.01
    # $20 / 5 miles = $4.00/mile
    assert abs(row1["fare_per_mile"] - 4.0) < 0.01


def test_revenue_features(spark):
    """Verify tip percentage and total revenue calculations."""
    df = create_sample_df(spark)
    df_transformed = add_revenue_features(df)

    row1 = df_transformed.collect()[0]
    # $4 tip on $20 fare = 20.0%
    assert abs(row1["tip_percentage"] - 20.0) < 0.01
    assert row1["total_revenue"] == 25.0


def test_day_type_features(spark):
    """Verify weekend and peak-hour flags."""
    df = create_sample_df(spark)
    df_with_time = add_time_features(df)
    df_transformed = add_day_type_features(df_with_time)

    rows = df_transformed.collect()
    # Monday 8:30 AM -> Weekday, is_peak_hour=True, is_weekend=False
    assert rows[0]["day_type"] == "Weekday"
    assert rows[0]["is_weekend"] is False
    assert rows[0]["is_peak_hour"] is True

    # Sunday 2:00 PM -> Weekend, is_peak_hour=False, is_weekend=True
    assert rows[1]["day_type"] == "Weekend"
    assert rows[1]["is_weekend"] is True
    assert rows[1]["is_peak_hour"] is False
