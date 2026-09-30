"""
src/ingestion.py — Data Ingestion Module
=========================================
Downloads NYC Yellow Taxi Parquet data from the TLC website and provides
Spark-based readers for both local and Databricks environments.
"""

import os
import logging
import requests
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

DEFAULT_YEAR = 2024
DEFAULT_MONTH = 1
DATA_DIR = os.environ.get("NYC_TAXI_DATA_DIR", os.path.join("data"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------
def download_file(url: str, dest: str, chunk_size: int = 8192) -> str:
    """Stream-download a file from *url* to *dest*. Returns the dest path."""
    Path(os.path.dirname(dest)).mkdir(parents=True, exist_ok=True)
    if os.path.exists(dest):
        logger.info("File already exists, skipping download: %s", dest)
        return dest

    logger.info("Downloading %s → %s", url, dest)
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    with open(dest, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            fh.write(chunk)

    size_mb = os.path.getsize(dest) / (1024 * 1024)
    logger.info("Downloaded %.1f MB → %s", size_mb, dest)
    return dest


def download_taxi_data(
    year: int = DEFAULT_YEAR,
    month: int = DEFAULT_MONTH,
    data_dir: str = DATA_DIR,
) -> str:
    """Download one month of Yellow Taxi Parquet data."""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    dest = os.path.join(data_dir, filename)
    return download_file(url, dest)


def download_zone_lookup(data_dir: str = DATA_DIR) -> str:
    """Download the TLC Taxi Zone Lookup CSV."""
    dest = os.path.join(data_dir, "taxi_zone_lookup.csv")
    return download_file(ZONE_LOOKUP_URL, dest)


# ---------------------------------------------------------------------------
# Spark readers
# ---------------------------------------------------------------------------
def read_parquet_local(spark, path: str = None):
    """
    Read a Parquet file (or glob) into a Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
    path  : str – path or glob pattern, e.g. "data/*.parquet"

    Returns
    -------
    pyspark.sql.DataFrame
    """
    if path is None:
        path = os.path.join(DATA_DIR, "*.parquet")
    logger.info("Reading Parquet from: %s", path)
    df = spark.read.parquet(path)
    logger.info("Loaded %d records, %d columns", df.count(), len(df.columns))
    return df


def read_parquet_databricks(spark, dbfs_path: str = "dbfs:/FileStore/tables/yellow_tripdata_*.parquet"):
    """
    Read Parquet files from DBFS (Databricks File System).

    Parameters
    ----------
    spark     : SparkSession (provided by Databricks runtime)
    dbfs_path : str – DBFS glob path

    Returns
    -------
    pyspark.sql.DataFrame
    """
    logger.info("Reading Parquet from DBFS: %s", dbfs_path)
    df = spark.read.parquet(dbfs_path)
    record_count = df.count()
    logger.info("Loaded %d records from Databricks", record_count)
    return df


def read_zone_lookup(spark, path: str = None):
    """Read the Taxi Zone Lookup CSV into a Spark DataFrame."""
    if path is None:
        path = os.path.join(DATA_DIR, "taxi_zone_lookup.csv")
    logger.info("Reading zone lookup from: %s", path)
    df = spark.read.csv(path, header=True, inferSchema=True)
    logger.info("Zone lookup loaded: %d zones", df.count())
    return df


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("=== NYC Taxi Data Ingestion ===")
    parquet_path = download_taxi_data()
    zone_path = download_zone_lookup()
    logger.info("Ingestion complete.")
    logger.info("  Parquet : %s", parquet_path)
    logger.info("  Zones   : %s", zone_path)
