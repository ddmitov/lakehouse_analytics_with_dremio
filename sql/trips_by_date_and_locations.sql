SELECT
    TO_DATE(tpep_pickup_datetime) AS trip_date,
    PULocationID AS start_location,
    DOLocationID AS end_location,
    CAST(RatecodeID AS INTEGER) AS rate_code_id,
    COUNT(total_amount) AS trips_number,
    CAST(SUM(total_amount) AS INTEGER) trips_income
FROM s3."dmitov-nyc-taxi".yellow."2018"
WHERE
    RatecodeID BETWEEN 1 AND 6
    AND tpep_pickup_datetime >= '2018-01-01'
    AND tpep_pickup_datetime <= '2018-12-31'
GROUP BY
    trip_date,
    start_location,
    end_location,
    rate_code_id
UNION
SELECT
    TO_DATE(tpep_pickup_datetime) AS trip_date,
    PULocationID AS start_location,
    DOLocationID AS end_location,
    CAST(RatecodeID AS INTEGER) AS rate_code_id,
    COUNT(total_amount) AS trips_number,
    CAST(SUM(total_amount) AS INTEGER) trips_income
FROM s3."dmitov-nyc-taxi".yellow."2019"
WHERE
    RatecodeID BETWEEN 1 AND 6
    AND tpep_pickup_datetime >= '2019-01-01'
    AND tpep_pickup_datetime <= '2019-12-31'
GROUP BY
    trip_date,
    start_location,
    end_location,
    rate_code_id
UNION
SELECT
    TO_DATE(tpep_pickup_datetime) AS trip_date,
    PULocationID AS start_location,
    DOLocationID AS end_location,
    CAST(RatecodeID AS INTEGER) AS rate_code_id,
    COUNT(total_amount) AS trips_number,
    CAST(SUM(total_amount) AS INTEGER) trips_income
FROM s3."dmitov-nyc-taxi".yellow."2020"
WHERE
    RatecodeID BETWEEN 1 AND 6
    AND tpep_pickup_datetime >= '2020-01-01'
    AND tpep_pickup_datetime <= '2020-12-31'
GROUP BY
    trip_date,
    start_location,
    end_location,
    rate_code_id
UNION
SELECT
    TO_DATE(tpep_pickup_datetime) AS trip_date,
    PULocationID AS start_location,
    DOLocationID AS end_location,
    CAST(RatecodeID AS INTEGER) AS rate_code_id,
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
    end_location,
    rate_code_id
UNION
SELECT
    TO_DATE(tpep_pickup_datetime) AS trip_date,
    PULocationID AS start_location,
    DOLocationID AS end_location,
    CAST(RatecodeID AS INTEGER) AS rate_code_id,
    COUNT(total_amount) AS trips_number,
    CAST(SUM(total_amount) AS INTEGER) trips_income
FROM s3."dmitov-nyc-taxi".yellow."2022"
WHERE
    RatecodeID BETWEEN 1 AND 6
    AND tpep_pickup_datetime >= '2022-01-01'
    AND tpep_pickup_datetime <= '2022-12-31'
GROUP BY
    trip_date,
    start_location,
    end_location,
    rate_code_id