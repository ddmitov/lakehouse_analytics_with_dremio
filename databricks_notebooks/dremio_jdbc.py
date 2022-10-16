# Databricks notebook source
# MAGIC %md
# MAGIC ### Read Data from Dremio Cloud using JDBC

# COMMAND ----------

import os
import pandas as pd

# COMMAND ----------

# dremio_project_id = dbutils.secrets.get(scope = 'dremio', key = 'project_id')
# dremio_token = dbutils.secrets.get(scope = 'dremio', key = 'token')

dremio_project_id = os.getenv('DREMIO_PROJECT_ID')
dremio_token = os.getenv('DREMIO_TOKEN')

dremio_url = (
    'jdbc:dremio:direct=sql.dremio.cloud:443;' +
    'ssl=true;' +
    'PROJECT_ID={};'.format(dremio_project_id) +
    'token_type=personal_access_token;' +
    'username=;' +
    'password={};'.format(dremio_token)
)

DREMIO_QUERY = '''
    SELECT
        trip_date,
        CAST(SUM(trips_number) AS INTEGER) AS daily_trips
    FROM "aggregations"."trips_by_date_and_locations"
    WHERE
        trip_date >= \'2018-01-01\'
        AND trip_date <= \'2022-06-30\'
    GROUP BY trip_date
    ORDER BY trip_date ASC
'''

dremio_spark_dataframe = spark.read.format('jdbc').options(
    driver='com.dremio.jdbc.Driver',
    url=dremio_url,
    query=DREMIO_QUERY
).load()

dremio_dataframe = dremio_spark_dataframe.toPandas()

display(dremio_dataframe)
