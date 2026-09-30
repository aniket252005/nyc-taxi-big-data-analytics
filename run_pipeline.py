"""
run_pipeline.py — End-to-End NYC Taxi Analytics Pipeline Runner
================================================================
Orchestrates Bronze (Ingestion) -> Silver (Cleaning & Quality) -> Gold (EDA & Analytics)
pipeline in a single executable command.

Usage:
    python run_pipeline.py
    python run_pipeline.py --year 2024 --month 1
    python run_pipeline.py --skip-download
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("nyc_taxi_pipeline")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the end-to-end NYC Taxi Big Data Analytics pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--year", type=int, default=2024, help="Year of the yellow taxi dataset."
    )
    parser.add_argument(
        "--month", type=int, default=1, help="Month of the yellow taxi dataset."
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Directory to store raw, silver, and gold datasets.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip data download and use existing files in data-dir.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    start_time = datetime.now()
    logger.info("=====================================================================")
    logger.info("   🚕 NYC YELLOW TAXI BIG DATA ANALYTICS PIPELINE")
    logger.info("=====================================================================")
    logger.info(f"Target dataset: Year={args.year}, Month={args.month}")
    logger.info(f"Data directory: {os.path.abspath(args.data_dir)}")

    # 1. Download
    if not args.skip_download:
        logger.info("\n>>> STEP 1: Ingestion & Download")
        from src.ingestion import download_taxi_data, download_zone_lookup

        parquet_file = download_taxi_data(
            year=args.year, month=args.month, data_dir=args.data_dir
        )
        zone_file = download_zone_lookup(data_dir=args.data_dir)
        logger.info(f"Data files ready:\n  - Parquet: {parquet_file}\n  - Zones: {zone_file}")
    else:
        logger.info("\n>>> STEP 1: Ingestion (Download skipped by user)")

    # 2. Initialize Spark
    logger.info("\n>>> Initializing Apache Spark Session ...")
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        logger.error(
            "PySpark is not installed in the current environment.\n"
            "Please install dependencies with: pip install -r requirements.txt"
        )
        sys.exit(1)

    spark = (
        SparkSession.builder
        .appName("NYC-Yellow-Taxi-Pipeline")
        .master("local[*]")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logger.info(f"SparkSession created successfully (Spark {spark.version})")

    # 3. Bronze Layer
    logger.info("\n>>> STEP 2: Loading Bronze Layer")
    parquet_path = os.path.join(args.data_dir, f"yellow_tripdata_{args.year}-{args.month:02d}.parquet")
    if not os.path.exists(parquet_path):
        parquet_path = os.path.join(args.data_dir, "*.parquet")

    df_bronze = spark.read.parquet(parquet_path)
    total_bronze = df_bronze.count()
    logger.info(f"Bronze layer loaded: {total_bronze:,} records, {len(df_bronze.columns)} columns")

    # 4. Data Quality & Silver Layer
    logger.info("\n>>> STEP 3: Data Quality Assessment & Cleaning (Silver Layer)")
    from src.cleaning import (
        validate_schema,
        add_validation_flags,
        build_quality_summary,
        clean_dataframe,
    )

    schema_report = validate_schema(df_bronze)
    logger.info(f"Schema validation: {'PASSED' if schema_report['schema_valid'] else 'WARNING'}")

    df_flagged = add_validation_flags(df_bronze)
    summary = build_quality_summary(df_flagged)

    logger.info("\n--- Quality Summary Metrics ---")
    logger.info(f"  Total records   : {summary['total_records']:,}")
    logger.info(f"  Valid records   : {summary['valid_records']:,}")
    logger.info(f"  Invalid records : {summary['invalid_records']:,} ({summary['pct_removed']}%)")

    df_silver = clean_dataframe(df_bronze)
    silver_path = os.path.join(args.data_dir, "silver")
    df_silver.write.mode("overwrite").parquet(silver_path)
    logger.info(f"Silver dataset written to: {silver_path}")

    # 5. Feature Engineering
    logger.info("\n>>> STEP 4: Feature Engineering")
    from src.transformations import apply_all_transformations

    df_enriched = apply_all_transformations(df_silver)
    df_enriched.createOrReplaceTempView("taxi_trips")
    logger.info(f"Enriched DataFrame ready with {len(df_enriched.columns)} features.")

    # 6. Analytics & Insights
    logger.info("\n>>> STEP 5: Running Analytics & Aggregations")
    from src.analysis import (
        hourly_demand,
        daily_demand,
        peak_vs_nonpeak,
        weekend_vs_weekday,
        top_pickup_zones,
        top_dropoff_zones,
        revenue_by_hour,
        revenue_by_zone,
        payment_type_distribution,
        trip_distance_distribution,
        rank_zones_by_revenue,
        generate_key_insights,
    )

    hourly = hourly_demand(df_enriched)
    daily = daily_demand(df_enriched)
    peak = peak_vs_nonpeak(df_enriched)
    wk = weekend_vs_weekday(df_enriched)
    top_pu = top_pickup_zones(df_enriched, n=10)
    top_do = top_dropoff_zones(df_enriched, n=10)
    rev_hr = revenue_by_hour(df_enriched)
    rev_zone = revenue_by_zone(df_enriched, n=10)
    pay = payment_type_distribution(df_enriched)
    dist = trip_distance_distribution(df_enriched)
    ranked = rank_zones_by_revenue(df_enriched)

    insights = generate_key_insights(df_enriched)
    logger.info("\n--- Key Analytical KPIs ---")
    for k, v in insights.items():
        logger.info(f"  {k:<28}: {v}")

    # 7. Gold Layer
    logger.info("\n>>> STEP 6: Exporting Gold Layer Tables")
    gold_dir = os.path.join(args.data_dir, "gold")
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

    for name, table_df in gold_tables.items():
        out_path = os.path.join(gold_dir, name)
        table_df.write.mode("overwrite").parquet(out_path)
        logger.info(f"  Exported {name} -> {out_path}")

    duration = datetime.now() - start_time
    logger.info("\n=====================================================================")
    logger.info(f"   🎉 PIPELINE COMPLETED SUCCESSFULLY in {duration.total_seconds():.1f}s")
    logger.info("=====================================================================")


if __name__ == "__main__":
    main()
