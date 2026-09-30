-- =============================================================================
-- zone_analysis.sql — Pickup & Drop-off Zone Analytics
-- =============================================================================

-- 1. Top 15 pickup zones by trip count
SELECT
    PULocationID,
    COUNT(*)                         AS trip_count,
    ROUND(SUM(total_revenue), 2)     AS total_revenue,
    ROUND(AVG(fare_amount), 2)       AS avg_fare,
    ROUND(AVG(trip_distance), 2)     AS avg_distance
FROM taxi_trips
GROUP BY PULocationID
ORDER BY trip_count DESC
LIMIT 15;


-- 2. Top 15 drop-off zones
SELECT
    DOLocationID,
    COUNT(*)                         AS trip_count,
    ROUND(AVG(fare_amount), 2)       AS avg_fare
FROM taxi_trips
GROUP BY DOLocationID
ORDER BY trip_count DESC
LIMIT 15;


-- 3. Zone revenue ranking with cumulative percentage (window)
WITH zone_rev AS (
    SELECT
        PULocationID,
        COUNT(*)                         AS trip_count,
        ROUND(SUM(total_revenue), 2)     AS total_revenue
    FROM taxi_trips
    GROUP BY PULocationID
)
SELECT
    PULocationID,
    trip_count,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC)                AS rev_rank,
    ROUND(
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC
                                  ROWS BETWEEN UNBOUNDED PRECEDING
                                  AND CURRENT ROW)
        * 100.0 / SUM(total_revenue) OVER (),
        2
    )                                                        AS cumulative_pct
FROM zone_rev
ORDER BY rev_rank
LIMIT 20;


-- 4. Top pickup-dropoff pairs
SELECT
    PULocationID,
    DOLocationID,
    COUNT(*)                             AS trip_count,
    ROUND(AVG(fare_amount), 2)           AS avg_fare,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration
FROM taxi_trips
GROUP BY PULocationID, DOLocationID
HAVING COUNT(*) > 50
ORDER BY trip_count DESC
LIMIT 15;


-- 5. High-demand zones (above-average trip count)
WITH zone_demand AS (
    SELECT
        PULocationID,
        COUNT(*) AS trip_count
    FROM taxi_trips
    GROUP BY PULocationID
),
avg_demand AS (
    SELECT AVG(trip_count) AS avg_trips FROM zone_demand
)
SELECT
    zd.PULocationID,
    zd.trip_count,
    ROUND(ad.avg_trips, 0)                            AS overall_avg,
    ROUND((zd.trip_count - ad.avg_trips) / ad.avg_trips * 100, 1) AS pct_above_avg
FROM zone_demand zd
CROSS JOIN avg_demand ad
WHERE zd.trip_count > ad.avg_trips
ORDER BY zd.trip_count DESC
LIMIT 15;
