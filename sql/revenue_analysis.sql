-- =============================================================================
-- revenue_analysis.sql — Revenue & Fare Analytics
-- =============================================================================

-- 1. Revenue by hour
SELECT
    pickup_hour,
    ROUND(SUM(total_revenue), 2)   AS total_revenue,
    ROUND(AVG(total_revenue), 2)   AS avg_revenue_per_trip,
    COUNT(*)                       AS trip_count
FROM taxi_trips
GROUP BY pickup_hour
ORDER BY pickup_hour;


-- 2. Revenue by payment type
SELECT
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Other'
    END                             AS payment_method,
    COUNT(*)                        AS trip_count,
    ROUND(SUM(total_revenue), 2)    AS total_revenue,
    ROUND(AVG(tip_percentage), 2)   AS avg_tip_pct,
    ROUND(AVG(fare_amount), 2)      AS avg_fare
FROM taxi_trips
GROUP BY payment_type
ORDER BY total_revenue DESC;


-- 3. Fare distribution buckets
SELECT
    CASE
        WHEN fare_amount < 10  THEN '$0–$10'
        WHEN fare_amount < 20  THEN '$10–$20'
        WHEN fare_amount < 30  THEN '$20–$30'
        WHEN fare_amount < 50  THEN '$30–$50'
        WHEN fare_amount < 100 THEN '$50–$100'
        ELSE '$100+'
    END                             AS fare_bucket,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(tip_percentage), 2)   AS avg_tip_pct
FROM taxi_trips
GROUP BY fare_bucket
ORDER BY fare_bucket;


-- 4. Tip analysis — credit card vs cash
SELECT
    CASE payment_type WHEN 1 THEN 'Credit Card' WHEN 2 THEN 'Cash' ELSE 'Other' END AS method,
    COUNT(*)                          AS trip_count,
    ROUND(AVG(tip_amount), 2)         AS avg_tip,
    ROUND(AVG(tip_percentage), 2)     AS avg_tip_pct,
    ROUND(PERCENTILE(tip_percentage, 0.5), 2) AS median_tip_pct
FROM taxi_trips
WHERE payment_type IN (1, 2)
GROUP BY payment_type
ORDER BY method;


-- 5. Revenue trend by day (daily revenue with 7-day moving average)
WITH daily_rev AS (
    SELECT
        DATE(tpep_pickup_datetime) AS trip_date,
        ROUND(SUM(total_revenue), 2) AS daily_revenue,
        COUNT(*)                     AS trip_count
    FROM taxi_trips
    GROUP BY DATE(tpep_pickup_datetime)
)
SELECT
    trip_date,
    daily_revenue,
    trip_count,
    ROUND(
        AVG(daily_revenue) OVER (
            ORDER BY trip_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2
    ) AS moving_avg_7d
FROM daily_rev
ORDER BY trip_date;
