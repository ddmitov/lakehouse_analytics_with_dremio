# Databricks notebook source
# MAGIC %md
# MAGIC ### Create External Delta Lake Table for Taxi Zones
# MAGIC 
# MAGIC #### Based on:
# MAGIC 
# MAGIC https://github.com/MrPowers/delta-examples/blob/master/notebooks/pyspark/create-table-delta-lake.ipynb

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
DIRECTORY = 'delta-lake-taxi-zones'

spark_dataframe = spark.createDataFrame(pandas_dataframe)

spark_dataframe.write.mode('overwrite').format('delta').save('s3a://{}:{}@{}/{}'.format(access_key, secret_key, BUCKET_NAME, DIRECTORY))

# COMMAND ----------

control_spark_dataframe = spark.read.format('delta').load('s3a://{}:{}@{}/{}'.format(access_key, secret_key, BUCKET_NAME, DIRECTORY))

display(control_spark_dataframe.sort('LocationID'))
