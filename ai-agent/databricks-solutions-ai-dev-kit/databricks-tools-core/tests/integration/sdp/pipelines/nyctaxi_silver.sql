-- Silver layer: Enriched NYC taxi trip data with calculated fields
CREATE OR REFRESH MATERIALIZED VIEW silver_trips
AS SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    fare_amount,
    pickup_zip,
    dropoff_zip,
    -- Calculated fields
    TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
    CASE
        WHEN fare_amount > 0 THEN fare_amount / NULLIF(trip_distance, 0)
        ELSE 0
    END AS fare_per_mile
FROM LIVE.bronze_trips
