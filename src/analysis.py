"""
src/analysis.py — Analytical Query Module
===========================================
Runs key analytical queries on the enriched NYC Taxi DataFrame using the
PySpark DataFrame API.  Results are returned as DataFrames (not collected
to the driver) so they remain scalable on full-size datasets.
"""

import logging
from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Demand analysis
# ---------------------------------------------------------------------------
def hourly_demand(df: DataFrame) -> DataFrame:
    """Total trips grouped by pickup hour."""
    return (
        df.groupBy("pickup_hour")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("pickup_hour")
    )


def daily_demand(df: DataFrame) -> DataFrame:
    """Total trips grouped by day of week (name)."""
    return (
        df.groupBy("pickup_day_name", "pickup_day_of_week")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("pickup_day_of_week")
    )


def peak_vs_nonpeak(df: DataFrame) -> DataFrame:
    """Compare trip counts and avg fare in peak vs non-peak hours."""
    return (
        df.groupBy("is_peak_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        )
        .orderBy("is_peak_hour")
    )


def weekend_vs_weekday(df: DataFrame) -> DataFrame:
    """Compare weekend vs weekday demand and revenue."""
    return (
        df.groupBy("day_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
        )
        .orderBy("day_type")
    )


# ---------------------------------------------------------------------------
# Zone analysis
# ---------------------------------------------------------------------------
def top_pickup_zones(df: DataFrame, n: int = 10) -> DataFrame:
    """Top N pickup zones by trip count."""
    return (
        df.groupBy("PULocationID")
        .agg(F.count("*").alias("trip_count"))
        .orderBy(F.desc("trip_count"))
        .limit(n)
    )


def top_dropoff_zones(df: DataFrame, n: int = 10) -> DataFrame:
    """Top N drop-off zones by trip count."""
    return (
        df.groupBy("DOLocationID")
        .agg(F.count("*").alias("trip_count"))
        .orderBy(F.desc("trip_count"))
        .limit(n)
    )


def revenue_by_zone(df: DataFrame, n: int = 10) -> DataFrame:
    """Top N pickup zones by total revenue."""
    return (
        df.groupBy("PULocationID")
        .agg(
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.count("*").alias("trip_count"),
        )
        .orderBy(F.desc("total_revenue"))
        .limit(n)
    )


def avg_fare_by_zone(df: DataFrame) -> DataFrame:
    """Average fare per pickup zone (all zones)."""
    return (
        df.groupBy("PULocationID")
        .agg(
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.count("*").alias("trip_count"),
        )
        .orderBy(F.desc("avg_fare"))
    )


# ---------------------------------------------------------------------------
# Revenue analysis
# ---------------------------------------------------------------------------
def revenue_by_hour(df: DataFrame) -> DataFrame:
    """Total and average revenue by pickup hour."""
    return (
        df.groupBy("pickup_hour")
        .agg(
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("total_revenue"), 2).alias("avg_revenue"),
            F.count("*").alias("trip_count"),
        )
        .orderBy("pickup_hour")
    )


def payment_type_distribution(df: DataFrame) -> DataFrame:
    """Trip count and tip behavior by payment type."""
    payment_labels = {1: "Credit Card", 2: "Cash", 3: "No Charge", 4: "Dispute", 5: "Unknown"}
    # Use a CASE WHEN via PySpark
    label_expr = F.coalesce(
        *[
            F.when(F.col("payment_type") == k, F.lit(v))
            for k, v in payment_labels.items()
        ],
        F.lit("Other"),
    )
    return (
        df.withColumn("payment_label", label_expr)
        .groupBy("payment_label")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
        )
        .orderBy(F.desc("trip_count"))
    )


# ---------------------------------------------------------------------------
# Trip patterns
# ---------------------------------------------------------------------------
def trip_distance_distribution(df: DataFrame) -> DataFrame:
    """Bucket trips by distance range."""
    return (
        df.withColumn(
            "distance_bucket",
            F.when(F.col("trip_distance") < 1, "< 1 mi")
            .when(F.col("trip_distance") < 3, "1–3 mi")
            .when(F.col("trip_distance") < 5, "3–5 mi")
            .when(F.col("trip_distance") < 10, "5–10 mi")
            .when(F.col("trip_distance") < 20, "10–20 mi")
            .otherwise("20+ mi"),
        )
        .groupBy("distance_bucket")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        )
        .orderBy("distance_bucket")
    )


# ---------------------------------------------------------------------------
# Window-function analysis
# ---------------------------------------------------------------------------
def rank_zones_by_revenue(df: DataFrame) -> DataFrame:
    """Rank pickup zones by total revenue using a window function."""
    zone_revenue = (
        df.groupBy("PULocationID")
        .agg(
            F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
            F.count("*").alias("trip_count"),
        )
    )
    w = Window.orderBy(F.desc("total_revenue"))
    return (
        zone_revenue
        .withColumn("revenue_rank", F.rank().over(w))
        .withColumn("dense_rank", F.dense_rank().over(w))
        .orderBy("revenue_rank")
    )


# ---------------------------------------------------------------------------
# Key insights generator
# ---------------------------------------------------------------------------
def generate_key_insights(df: DataFrame) -> dict:
    """
    Compute headline KPIs from the enriched DataFrame.
    Returns a dict of metric-name → value.

    WARNING: calls .collect() on small aggregates only.
    """
    logger.info("Generating key insights …")
    row = df.agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
    ).collect()[0]

    # Peak demand hour
    peak_hour_row = (
        df.groupBy("pickup_hour")
        .agg(F.count("*").alias("cnt"))
        .orderBy(F.desc("cnt"))
        .limit(1)
        .collect()[0]
    )

    # Highest-revenue hour
    rev_hour_row = (
        df.groupBy("pickup_hour")
        .agg(F.round(F.sum("total_revenue"), 2).alias("rev"))
        .orderBy(F.desc("rev"))
        .limit(1)
        .collect()[0]
    )

    # Highest-demand day
    peak_day_row = (
        df.groupBy("pickup_day_name")
        .agg(F.count("*").alias("cnt"))
        .orderBy(F.desc("cnt"))
        .limit(1)
        .collect()[0]
    )

    # Top pickup zone (by count)
    top_pu_row = (
        df.groupBy("PULocationID")
        .agg(F.count("*").alias("cnt"))
        .orderBy(F.desc("cnt"))
        .limit(1)
        .collect()[0]
    )

    # Weekend vs weekday avg trips
    day_type_rows = (
        df.groupBy("day_type")
        .agg(F.count("*").alias("cnt"))
        .collect()
    )
    day_type_map = {r["day_type"]: r["cnt"] for r in day_type_rows}

    insights = {
        "total_trips": row["total_trips"],
        "total_revenue": row["total_revenue"],
        "avg_fare": row["avg_fare"],
        "avg_distance_miles": row["avg_distance"],
        "avg_duration_minutes": row["avg_duration_min"],
        "avg_tip_pct": row["avg_tip_pct"],
        "peak_demand_hour": peak_hour_row["pickup_hour"],
        "peak_demand_hour_trips": peak_hour_row["cnt"],
        "highest_revenue_hour": rev_hour_row["pickup_hour"],
        "highest_revenue_hour_total": rev_hour_row["rev"],
        "highest_demand_day": peak_day_row["pickup_day_name"],
        "highest_demand_day_trips": peak_day_row["cnt"],
        "top_pickup_zone_id": top_pu_row["PULocationID"],
        "top_pickup_zone_trips": top_pu_row["cnt"],
        "weekday_trips": day_type_map.get("Weekday", 0),
        "weekend_trips": day_type_map.get("Weekend", 0),
    }

    logger.info("=== Key Insights ===")
    for k, v in insights.items():
        logger.info("  %-30s : %s", k, v)

    return insights
