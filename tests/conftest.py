"""
tests/conftest.py — Pytest Configuration and Shared Fixtures
============================================================
Provides a lightweight SparkSession fixture for unit testing PySpark code.
"""

import pytest

@pytest.fixture(scope="session")
def spark():
    """Create a minimal local SparkSession for testing."""
    try:
        from pyspark.sql import SparkSession
        spark_session = (
            SparkSession.builder
            .appName("NYC-Taxi-Tests")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "America/New_York")
            .getOrCreate()
        )
        yield spark_session
        spark_session.stop()
    except Exception as exc:
        pytest.skip(f"Local PySpark session unavailable: {exc}")
