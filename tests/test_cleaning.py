"""
tests/test_cleaning.py — Unit Tests for Data Quality & Cleaning Module
"""

import pytest
pyspark = pytest.importorskip("pyspark")
from datetime import datetime
from src.cleaning import (
    EXPECTED_COLUMNS,
    validate_schema,
    add_validation_flags,
    clean_dataframe,
)


def create_dummy_df(spark, rows):
    """Helper to build a small DataFrame matching the TLC schema structure."""
    from pyspark.sql.types import (
        StructType, StructField, IntegerType, DoubleType,
        TimestampType, StringType,
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
    return spark.createDataFrame(rows, schema=schema)


def test_validate_schema_success(spark):
    """Test that schema validation passes when all expected columns exist."""
    df = create_dummy_df(spark, [])
    report = validate_schema(df)
    assert report["schema_valid"] is True
    assert len(report["missing_columns"]) == 0


def test_validate_schema_missing_column(spark):
    """Test that schema validation detects missing columns."""
    df = create_dummy_df(spark, []).drop("passenger_count")
    report = validate_schema(df)
    assert report["schema_valid"] is False
    assert "passenger_count" in report["missing_columns"]


def test_validation_flags_and_cleaning(spark):
    """Test flagging of invalid rows and filtering in clean_dataframe."""
    valid_row = (
        1,
        datetime(2024, 1, 15, 10, 0, 0),
        datetime(2024, 1, 15, 10, 15, 0),
        2,      # passenger_count
        2.5,    # trip_distance
        1,
        "N",
        161,    # PULocationID
        237,    # DOLocationID
        1,      # payment_type (Credit Card)
        12.0,   # fare_amount
        0.5,
        0.5,
        2.5,    # tip_amount
        0.0,
        1.0,
        16.5,   # total_amount
        2.5,
        0.0,
    )

    # Invalid: 0 passengers, negative fare, 0 distance
    invalid_row = (
        1,
        datetime(2024, 1, 15, 10, 0, 0),
        datetime(2024, 1, 15, 10, 0, 5),  # 5 sec duration (< 60s)
        0,      # invalid passenger_count (< 1)
        0.0,    # invalid trip_distance (< 0.1)
        1,
        "N",
        161,
        237,
        1,
        -5.0,   # invalid negative fare
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        -5.0,   # invalid total
        0.0,
        0.0,
    )

    df = create_dummy_df(spark, [valid_row, invalid_row])
    df_flagged = add_validation_flags(df)

    flagged_rows = df_flagged.collect()
    assert len(flagged_rows) == 2
    # First row should be valid
    assert flagged_rows[0]["_is_invalid"] is False
    # Second row should be invalid
    assert flagged_rows[1]["_is_invalid"] is True

    # Test clean_dataframe drops the invalid row
    df_clean = clean_dataframe(df)
    assert df_clean.count() == 1
