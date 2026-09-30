-- =============================================================================
-- 04 — Spark SQL Analysis
-- =============================================================================
-- Run these queries after registering the enriched DataFrame as a temp view:
--     df_enriched.createOrReplaceTempView("taxi_trips")
--
-- Each query demonstrates a different Spark SQL feature.
-- On Databricks, switch the cell to %sql mode.
-- =============================================================================

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  1. Top 10 Pickup Zones  (GROUP BY + ORDER BY)                          ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

SELECT
    PULocationID,
    COUNT(*)            AS trip_count,
    ROUND(SUM(total_revenue), 2) AS total_revenue
FROM taxi_trips
GROUP BY PULocationID
ORDER BY trip_count DESC
LIMIT 10;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  2. Top 10 Drop-off Zones                                               ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

SELECT
    DOLocationID,
    COUNT(*)            AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM taxi_trips
GROUP BY DOLocationID
ORDER BY trip_count DESC
LIMIT 10;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  3. Hourly Demand  (Aggregation + Date function)                        ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

SELECT
    pickup_hour,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(fare_amount), 2)      AS avg_fare,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration
FROM taxi_trips
GROUP BY pickup_hour
ORDER BY pickup_hour;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  4. Daily Demand                                                        ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

SELECT
    pickup_day_name,
    pickup_day_of_week,
    COUNT(*)                   AS trip_count,
    ROUND(SUM(total_revenue), 2) AS total_revenue
FROM taxi_trips
GROUP BY pickup_day_name, pickup_day_of_week
ORDER BY pickup_day_of_week;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  5. Weekend vs Weekday  (CASE WHEN)                                     ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

SELECT
    CASE
        WHEN is_weekend = true THEN 'Weekend'
        ELSE 'Weekday'
    END                            AS day_type,
    COUNT(*)                       AS trip_count,
    ROUND(AVG(fare_amount), 2)     AS avg_fare,
    ROUND(AVG(trip_distance), 2)   AS avg_distance,
    ROUND(AVG(tip_percentage), 2)  AS avg_tip_pct
FROM taxi_trips
GROUP BY is_weekend
ORDER BY day_type;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  6. Revenue by Hour  (SUM + Aggregation)                                ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

SELECT
    pickup_hour,
    ROUND(SUM(total_revenue), 2)   AS total_revenue,
    ROUND(AVG(total_revenue), 2)   AS avg_revenue,
    COUNT(*)                       AS trip_count
FROM taxi_trips
GROUP BY pickup_hour
ORDER BY pickup_hour;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  7. Revenue by Zone  (Top 10)                                           ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

SELECT
    PULocationID,
    ROUND(SUM(total_revenue), 2)   AS total_revenue,
    ROUND(AVG(fare_amount), 2)     AS avg_fare,
    COUNT(*)                       AS trip_count
FROM taxi_trips
GROUP BY PULocationID
ORDER BY total_revenue DESC
LIMIT 10;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  8. Average Fare by Zone  (CTE)                                         ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

WITH zone_stats AS (
    SELECT
        PULocationID,
        COUNT(*)                     AS trip_count,
        ROUND(AVG(fare_amount), 2)   AS avg_fare,
        ROUND(AVG(trip_distance), 2) AS avg_distance,
        ROUND(SUM(total_revenue), 2) AS total_revenue
    FROM taxi_trips
    GROUP BY PULocationID
)
SELECT *
FROM zone_stats
WHERE trip_count > 100
ORDER BY avg_fare DESC
LIMIT 15;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  9. Tip Percentage by Payment Type  (CASE WHEN + GROUP BY)              ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

SELECT
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Other'
    END                             AS payment_label,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(tip_percentage), 2)   AS avg_tip_pct,
    ROUND(SUM(total_revenue), 2)    AS total_revenue
FROM taxi_trips
GROUP BY payment_type
ORDER BY trip_count DESC;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  10. Zone Revenue Ranking  (Window Function: RANK, DENSE_RANK)          ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

WITH zone_revenue AS (
    SELECT
        PULocationID,
        ROUND(SUM(total_revenue), 2) AS total_revenue,
        COUNT(*)                     AS trip_count
    FROM taxi_trips
    GROUP BY PULocationID
)
SELECT
    PULocationID,
    total_revenue,
    trip_count,
    RANK()       OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS dense_rank,
    ROUND(
        total_revenue * 100.0 / SUM(total_revenue) OVER (),
        2
    )                                                AS pct_of_total_revenue
FROM zone_revenue
ORDER BY revenue_rank
LIMIT 15;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  11. Peak Hour Demand Analysis  (CTE + Window + CASE WHEN)              ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

WITH hourly_stats AS (
    SELECT
        pickup_hour,
        is_peak_hour,
        COUNT(*)                         AS trip_count,
        ROUND(AVG(fare_amount), 2)       AS avg_fare,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration,
        ROUND(SUM(total_revenue), 2)     AS total_revenue
    FROM taxi_trips
    GROUP BY pickup_hour, is_peak_hour
),
ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY trip_count DESC) AS demand_rank
    FROM hourly_stats
)
SELECT
    pickup_hour,
    CASE WHEN is_peak_hour THEN '🔴 Peak' ELSE '🟢 Off-Peak' END AS period,
    trip_count,
    avg_fare,
    avg_duration,
    total_revenue,
    demand_rank
FROM ranked
ORDER BY pickup_hour;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  12. Pickup-Dropoff Zone Pair Analysis  (JOIN + Aggregation)            ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

SELECT
    PULocationID,
    DOLocationID,
    COUNT(*)                         AS trip_count,
    ROUND(AVG(fare_amount), 2)       AS avg_fare,
    ROUND(AVG(trip_distance), 2)     AS avg_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration
FROM taxi_trips
GROUP BY PULocationID, DOLocationID
HAVING COUNT(*) > 50
ORDER BY trip_count DESC
LIMIT 15;
