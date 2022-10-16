# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Parquet File for Taxi Zones

# COMMAND ----------

import pandas as pd
import io
import os
import requests

# COMMAND ----------

url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'

web_content = requests.get(url).content
pandas_dataframe = pd.read_csv(io.StringIO(web_content.decode('utf-8')))

# COMMAND ----------

display(pandas_dataframe)

# COMMAND ----------

# access_key = dbutils.secrets.get(scope = 'aws', key = 'access_key')
# secret_key = dbutils.secrets.get(scope = 'aws', key = 'secret_key')

access_key = os.getenv('AWS_ACCESS_KEY')
secret_key = os.getenv('AWS_SECRET_KEY')

BUCKET_NAME = 'dmitov-nyc-taxi'
PARQUET_DIRECTORY = 'parquet-taxi-zones'

spark_dataframe = spark.createDataFrame(pandas_dataframe)

# spark_dataframe.coalesce(1).write.mode('overwrite').format('parquet').save(
#     's3a://{}:{}@{}/{}'.format(access_key, secret_key, BUCKET_NAME, PARQUET_DIRECTORY)
# )

temporary_parquet_directory = '/FileStore/temp'
spark_dataframe.coalesce(1).write.parquet(temporary_parquet_directory)

files = dbutils.fs.ls(temporary_parquet_directory)

for file in files:
    if '.snappy.parquet' in file.name:
        dbutils.fs.cp(file.path, 's3a://{}:{}@{}/{}/2022/01.snappy.parquet'.format(access_key, secret_key, BUCKET_NAME, PARQUET_DIRECTORY))

dbutils.fs.rm(temporary_parquet_directory, recurse=True)

# COMMAND ----------

control_parquet_dataframe = spark.read.format('parquet').load(
    's3a://{}:{}@{}/{}/2022/01.snappy.parquet'.format(
        access_key, secret_key, BUCKET_NAME, PARQUET_DIRECTORY
    )
)

display(control_parquet_dataframe)
