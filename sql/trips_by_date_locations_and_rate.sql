SELECT
    s3.trip_date,
    delta_lake_start_location.Zone AS start_location,
    delta_lake_end_location.Zone AS end_location,
    postgre.Rate AS rate,
    s3.trips_number,
    s3.trips_income
FROM "aggregations"."trips_by_date_and_locations" s3
    LEFT JOIN "postgre"."rates"."rates" postgre ON
        postgre.RateCodeID = s3.rate_code_id
    LEFT JOIN s3."dmitov-nyc-taxi"."delta-lake-taxi-zones" delta_lake_start_location ON
        delta_lake_start_location.LocationID = s3.start_location
    LEFT JOIN s3."dmitov-nyc-taxi"."delta-lake-taxi-zones" delta_lake_end_location ON
        delta_lake_end_location.LocationID = s3.end_location