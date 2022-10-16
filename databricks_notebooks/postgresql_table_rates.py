# Databricks notebook source
# MAGIC %md
# MAGIC ### Create PostgreSQL Table for Taxi Rates

# COMMAND ----------

!pip install sqlalchemy

# COMMAND ----------

from sqlalchemy import create_engine
import pandas as pd

# COMMAND ----------

pandas_dataframe = pd.DataFrame(
    {
        'RateCodeID': [1, 2, 3, 4, 5, 6],
        'Rate': ['Standard rate', 'JFK', 'Newark', 'Nassau or Westchester', 'Negotiated fare', 'Group ride']
    }
)

display(pandas_dataframe)

# COMMAND ----------

POSTGRESQL_USERNAME = ''
POSTGRESQL_PASSWORD = ''
POSTGRESQL_HOSTNAME = ''
POSTGRESQL_SCHEMA = 'rates'

postgresql_engine = create_engine(f'postgresql://{POSTGRESQL_USERNAME}:{POSTGRESQL_PASSWORD}@{POSTGRESQL_HOSTNAME}/{POSTGRESQL_SCHEMA}')

# COMMAND ----------

pandas_dataframe.to_sql(
    'rates',
    con=postgresql_engine,
    schema=POSTGRESQL_SCHEMA,
    if_exists='replace',
    index=False
)

# COMMAND ----------

control_pandas_dataframe = pd.read_sql(
    'SELECT * FROM rates.rates',
    postgresql_engine
)

display(control_pandas_dataframe)
