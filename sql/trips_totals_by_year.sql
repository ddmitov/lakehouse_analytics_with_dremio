SELECT
    2018 AS "year",
    COUNT(*) AS total_trips_number,
    CAST(SUM(total_amount) AS INTEGER) AS total_income
FROM s3."dmitov-nyc-taxi".yellow."2018"
UNION
SELECT
    2019 AS "year",
    COUNT(*) AS total_trips_number,
    CAST(SUM(total_amount) AS INTEGER) AS total_income
FROM s3."dmitov-nyc-taxi".yellow."2019"
UNION
SELECT
    2020 AS "year",
    COUNT(*) AS total_trips_number,
    CAST(SUM(total_amount) AS INTEGER) AS total_income
FROM s3."dmitov-nyc-taxi".yellow."2020"
UNION
SELECT
    2021 AS "year",
    COUNT(*) AS total_trips_number,
    CAST(SUM(total_amount) AS INTEGER) AS total_income
FROM s3."dmitov-nyc-taxi".yellow."2021"
UNION
SELECT
    2022 AS "year",
    COUNT(*) AS total_trips_number,
    CAST(SUM(total_amount) AS INTEGER) AS total_income
FROM s3."dmitov-nyc-taxi".yellow."2022"