SELECT SUM(trips.trips_number) AS trips_total
FROM (
    SELECT COUNT(*) AS trips_number
    FROM s3."dmitov-nyc-taxi".yellow."2018"
    UNION
    SELECT COUNT(*) AS trips_number
    FROM s3."dmitov-nyc-taxi".yellow."2019"
    UNION
    SELECT COUNT(*) AS trips_number
    FROM s3."dmitov-nyc-taxi".yellow."2020"
    UNION
    SELECT COUNT(*) AS trips_number
    FROM s3."dmitov-nyc-taxi".yellow."2021"
    UNION
    SELECT COUNT(*) AS trips_number
    FROM s3."dmitov-nyc-taxi".yellow."2022"
) AS trips