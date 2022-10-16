SELECT
    TO_DATE(ds) AS trip_date,
    CAST(forecast AS INTEGER) AS forecasted_trips_number
FROM s3."dmitov-nyc-taxi"."parquet-forecast"
ORDER BY trip_date