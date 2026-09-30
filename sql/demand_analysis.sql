-- =============================================================================
-- demand_analysis.sql — Taxi Demand Analytics
-- =============================================================================
-- Prerequisite: register enriched DataFrame as temp view "taxi_trips"
-- =============================================================================

-- 1. Hourly demand with running total (window function)
SELECT
    pickup_hour,
    COUNT(*)                                             AS trip_count,
    ROUND(AVG(fare_amount), 2)                           AS avg_fare,
    SUM(COUNT(*)) OVER (ORDER BY pickup_hour
                        ROWS BETWEEN UNBOUNDED PRECEDING
                        AND CURRENT ROW)                 AS cumulative_trips
FROM taxi_trips
GROUP BY pickup_hour
ORDER BY pickup_hour;


-- 2. Day-of-week demand with percentage share
WITH daily AS (
    SELECT
        pickup_day_name,
        pickup_day_of_week,
        COUNT(*) AS trip_count
    FROM taxi_trips
    GROUP BY pickup_day_name, pickup_day_of_week
)
SELECT
    pickup_day_name,
    trip_count,
    ROUND(trip_count * 100.0 / SUM(trip_count) OVER (), 2) AS pct_share
FROM daily
ORDER BY pickup_day_of_week;


-- 3. Peak vs off-peak demand comparison
SELECT
    CASE WHEN is_peak_hour THEN 'Peak' ELSE 'Off-Peak' END AS period,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(fare_amount), 2)      AS avg_fare,
    ROUND(AVG(trip_distance), 2)    AS avg_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration
FROM taxi_trips
GROUP BY is_peak_hour
ORDER BY period;


-- 4. Weekend vs weekday hourly demand heatmap data
SELECT
    day_type,
    pickup_hour,
    COUNT(*)                   AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM taxi_trips
GROUP BY day_type, pickup_hour
ORDER BY day_type, pickup_hour;


-- 5. Top 5 busiest hours with rank
WITH hourly AS (
    SELECT pickup_hour, COUNT(*) AS trip_count
    FROM taxi_trips
    GROUP BY pickup_hour
)
SELECT
    pickup_hour,
    trip_count,
    RANK() OVER (ORDER BY trip_count DESC) AS demand_rank
FROM hourly
ORDER BY demand_rank
LIMIT 5;
