"""
tests/test_analysis.py — Unit Tests for Analytical Queries
"""

import pytest
pyspark = pytest.importorskip("pyspark")
from datetime import datetime
from src.transformations import apply_all_transformations
from src.analysis import (
    hourly_demand,
    daily_demand,
    peak_vs_nonpeak,
    weekend_vs_weekday,
    generate_key_insights,
)


def create_analytical_df(spark):
    """Create a sample enriched DataFrame for testing analysis functions."""
    from pyspark.sql.types import (
        StructType, StructField, IntegerType, DoubleType, TimestampType, StringType
    )

    schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True),
    ])

    rows = [
        (1, datetime(2024, 1, 15, 8, 0, 0), datetime(2024, 1, 15, 8, 20, 0), 1, 3.0, 1, "N", 161, 237, 1, 15.0, 0.0, 0.5, 3.0, 0.0, 1.0, 19.5, 2.5, 0.0),
        (2, datetime(2024, 1, 15, 8, 30, 0), datetime(2024, 1, 15, 8, 50, 0), 2, 4.0, 1, "N", 161, 142, 1, 18.0, 0.0, 0.5, 4.0, 0.0, 1.0, 23.5, 2.5, 0.0),
        (1, datetime(2024, 1, 20, 14, 0, 0), datetime(2024, 1, 20, 14, 30, 0), 1, 6.0, 1, "N", 230, 161, 2, 25.0, 0.0, 0.5, 0.0, 0.0, 1.0, 26.5, 2.5, 0.0),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    return apply_all_transformations(df)


def test_hourly_demand(spark):
    """Check hourly aggregation returns counts."""
    df = create_analytical_df(spark)
    res = hourly_demand(df).collect()
    # Should have hour 8 (count 2) and hour 14 (count 1)
    hour_map = {r["pickup_hour"]: r["trip_count"] for r in res}
    assert hour_map[8] == 2
    assert hour_map[14] == 1


def test_peak_vs_nonpeak(spark):
    """Check peak hour comparisons."""
    df = create_analytical_df(spark)
    res = peak_vs_nonpeak(df).collect()
    peak_map = {r["is_peak_hour"]: r["trip_count"] for r in res}
    assert peak_map[True] == 2
    assert peak_map[False] == 1


def test_generate_key_insights(spark):
    """Check KPI generation logic."""
    df = create_analytical_df(spark)
    insights = generate_key_insights(df)
    assert insights["total_trips"] == 3
    assert insights["peak_demand_hour"] == 8
    assert insights["top_pickup_zone_id"] == 161
