-- =============================================================================
-- performance_analysis.sql — Query Performance Comparisons
-- =============================================================================
-- These queries are designed to be run on Databricks with the full dataset.
-- Compare execution plans and runtimes between naive and optimized versions.
-- =============================================================================

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  Comparison 1: Aggregate-then-filter vs Filter-then-aggregate          │
-- └─────────────────────────────────────────────────────────────────────────┘

-- NAIVE: aggregate everything, then filter
-- Expected: full table scan + full shuffle, then filter at the end
SELECT
    PULocationID,
    pickup_hour,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_revenue), 2) AS revenue
FROM taxi_trips
GROUP BY PULocationID, pickup_hour
HAVING pickup_hour BETWEEN 7 AND 9
ORDER BY revenue DESC
LIMIT 10;

-- OPTIMIZED: filter first, then aggregate
-- Expected: partition pruning (if partitioned), less shuffle
SELECT
    PULocationID,
    pickup_hour,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_revenue), 2) AS revenue
FROM taxi_trips
WHERE pickup_hour BETWEEN 7 AND 9
GROUP BY PULocationID, pickup_hour
ORDER BY revenue DESC
LIMIT 10;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  Comparison 2: SELECT * vs column projection                           │
-- └─────────────────────────────────────────────────────────────────────────┘

-- NAIVE: read all columns
SELECT *
FROM taxi_trips
WHERE PULocationID = 132
LIMIT 100;

-- OPTIMIZED: read only needed columns (Parquet column pruning)
SELECT
    PULocationID,
    fare_amount,
    trip_distance,
    pickup_hour
FROM taxi_trips
WHERE PULocationID = 132
LIMIT 100;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  Comparison 3: Subquery vs CTE for readability & potential reuse       │
-- └─────────────────────────────────────────────────────────────────────────┘

-- SUBQUERY version
SELECT
    PULocationID,
    trip_count,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS rev_rank
FROM (
    SELECT
        PULocationID,
        COUNT(*)                         AS trip_count,
        ROUND(SUM(total_revenue), 2)     AS total_revenue
    FROM taxi_trips
    GROUP BY PULocationID
) sub
ORDER BY rev_rank
LIMIT 10;

-- CTE version (cleaner, same performance)
WITH zone_agg AS (
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
    RANK() OVER (ORDER BY total_revenue DESC) AS rev_rank
FROM zone_agg
ORDER BY rev_rank
LIMIT 10;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  Comparison 4: Efficient date filtering on partitioned data            │
-- └─────────────────────────────────────────────────────────────────────────┘

-- If data is partitioned by pickup_hour, this query benefits from
-- partition pruning.  On unpartitioned data, the performance is similar.

-- NAIVE: function on column prevents pushdown
SELECT COUNT(*)
FROM taxi_trips
WHERE HOUR(tpep_pickup_datetime) = 18;

-- OPTIMIZED: use pre-computed column (allows predicate pushdown)
SELECT COUNT(*)
FROM taxi_trips
WHERE pickup_hour = 18;
