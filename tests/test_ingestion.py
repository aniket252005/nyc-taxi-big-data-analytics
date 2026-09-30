"""
tests/test_ingestion.py — Unit Tests for Data Ingestion Module
"""

import os
from unittest.mock import patch, MagicMock
from src.ingestion import (
    BASE_URL,
    ZONE_LOOKUP_URL,
    download_taxi_data,
    download_zone_lookup,
)


def test_urls():
    """Verify expected base URLs."""
    assert "trip-data" in BASE_URL
    assert "taxi_zone_lookup.csv" in ZONE_LOOKUP_URL


@patch("src.ingestion.download_file")
def test_download_taxi_data_url_generation(mock_download):
    """Verify taxi dataset URL formatting."""
    mock_download.return_value = "dummy/path.parquet"
    res = download_taxi_data(year=2024, month=1, data_dir="test_dir")
    
    expected_filename = "yellow_tripdata_2024-01.parquet"
    expected_url = f"{BASE_URL}/{expected_filename}"
    expected_dest = os.path.join("test_dir", expected_filename)
    
    mock_download.assert_called_once_with(expected_url, expected_dest)
    assert res == "dummy/path.parquet"


@patch("src.ingestion.download_file")
def test_download_zone_lookup_url_generation(mock_download):
    """Verify taxi zone lookup URL and target path."""
    mock_download.return_value = "dummy/taxi_zone_lookup.csv"
    res = download_zone_lookup(data_dir="test_dir")
    
    expected_dest = os.path.join("test_dir", "taxi_zone_lookup.csv")
    mock_download.assert_called_once_with(ZONE_LOOKUP_URL, expected_dest)
    assert res == "dummy/taxi_zone_lookup.csv"
