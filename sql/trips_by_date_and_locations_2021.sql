SELECT
    TO_DATE(tpep_pickup_datetime) AS trip_date,
    PULocationID AS start_location,
    DOLocationID AS end_location,
    COUNT(total_amount) AS trips_number,
    CAST(SUM(total_amount) AS INTEGER) trips_income
FROM s3."dmitov-nyc-taxi".yellow."2021"
WHERE
    RatecodeID BETWEEN 1 AND 6
    AND tpep_pickup_datetime >= '2021-01-01'
    AND tpep_pickup_datetime <= '2021-12-31'
GROUP BY
    trip_date,
    start_location,
    end_location