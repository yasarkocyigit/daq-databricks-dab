-- Bronze layer: Read raw NYC taxi trip data
CREATE OR REFRESH STREAMING TABLE bronze_trips
AS SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    fare_amount,
    pickup_zip,
    dropoff_zip
FROM STREAM samples.nyctaxi.trips
