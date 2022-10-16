# Databricks notebook source
# MAGIC %md
# MAGIC ### Read Data from Dremio Cloud using Apache Arrow Flight
# MAGIC 
# MAGIC #### Based on:
# MAGIC 
# MAGIC https://github.com/dremio-hub/arrow-flight-client-examples/blob/main/python/example.py  

# COMMAND ----------

import os
import pandas as pd

from http.cookies import SimpleCookie
from pyarrow import flight

# COMMAND ----------

class DremioClientAuthMiddlewareFactory(flight.ClientMiddlewareFactory):
    """A factory that creates DremioClientAuthMiddleware(s)."""

    def __init__(self):
        self.call_credential = []

    def start_call(self, info):
        return DremioClientAuthMiddleware(self)

    def set_call_credential(self, call_credential):
        self.call_credential = call_credential

# COMMAND ----------

class DremioClientAuthMiddleware(flight.ClientMiddleware):
    """
    A ClientMiddleware that extracts the bearer token from 
    the authorization header returned by the Dremio 
    Flight Server Endpoint.

    Parameters
    ----------
    factory : ClientHeaderAuthMiddlewareFactory
        The factory to set call credentials if an
        authorization header with bearer token is
        returned by the Dremio server.
    """

    def __init__(self, factory):
        self.factory = factory

    def received_headers(self, headers):
        auth_header_key = 'authorization'
        authorization_header = []

        for key in headers:
            if key.lower() == auth_header_key:
                authorization_header = headers.get(auth_header_key)
        if not authorization_header:
            raise Exception('Did not receive authorization header back from server.')

        self.factory.set_call_credential([
            b'authorization', authorization_header[0].encode('utf-8')])

# COMMAND ----------

class CookieMiddlewareFactory(flight.ClientMiddlewareFactory):
    """A factory that creates CookieMiddleware(s)."""

    def __init__(self):
        self.cookies = {}

    def start_call(self, info):
        return CookieMiddleware(self)

# COMMAND ----------

class CookieMiddleware(flight.ClientMiddleware):
    """
    A ClientMiddleware that receives and retransmits cookies.
    For simplicity, this does not auto-expire cookies.

    Parameters
    ----------
    factory : CookieMiddlewareFactory
        The factory containing the currently cached cookies.
    """

    def __init__(self, factory):
        self.factory = factory

    def received_headers(self, headers):
        for key in headers:
            if key.lower() == 'set-cookie':
                cookie = SimpleCookie()

                for item in headers.get(key):
                    cookie.load(item)

                self.factory.cookies.update(cookie.items())

    def sending_headers(self):
        if self.factory.cookies:
            cookie_string = '; '.join("{!s}={!s}".format(key, val.value) for (key, val) in self.factory.cookies.items())
            return {b'cookie': cookie_string.encode('utf-8')}
        return {}

# COMMAND ----------

def dremio_data_downloader(host, port, dremio_token, query, engine):
    """
    Connects to Dremio Flight server endpoint with the provided credentials.
    It also runs the query and retrieves the result set.
    """

    try:
        scheme = "grpc+tls"

        connection_args = {}
        connection_args['disable_server_verification'] = True

        headers = []

        if engine:
            headers.append((b'routing_engine', engine.encode('utf-8')))

        client_cookie_middleware = CookieMiddlewareFactory()

        client = flight.FlightClient(
            "{}://{}:{}".format(scheme, host, port),
            middleware=[client_cookie_middleware],
            **connection_args
        )

        headers.append((b'authorization', "Bearer {}".format(dremio_token).encode('utf-8')))

        # Get the FlightInfo message
        # to retrieve the Ticket corresponding
        # to the query result set.
        options = flight.FlightCallOptions(headers=headers)

        flight_info = client.get_flight_info(
            flight.FlightDescriptor.for_command(query),
            options
        )

        print('[INFO] GetFlightInfo was successful')

        # Retrieve the result set as a stream of Arrow record batches.
        reader = client.do_get(flight_info.endpoints[0].ticket, options)

        print('[INFO] Reading query results from Dremio')

        dremio_pandas_dataframe = reader.read_pandas()

        return dremio_pandas_dataframe

    except Exception as exception:
        print("[ERROR] Exception: {}".format(repr(exception)))
        raise

# COMMAND ----------

DREMIO_HOST = 'data.dremio.cloud'
DREMIO_PORT = 443

# dremio_token = dbutils.secrets.get(scope = 'dremio', key = 'token')

dremio_token = os.getenv('DREMIO_TOKEN')

DREMIO_QUERY = '''
    SELECT
        trip_date,
        CAST(SUM(trips_number) AS INTEGER) AS daily_trips
    FROM "aggregations"."trips_by_date_and_locations"
    GROUP BY trip_date
    ORDER BY trip_date ASC;
'''

# Dremio engine to execute the query - optional parameter:
DREMIO_ENGINE = 'preview'

dremio_dataframe = dremio_data_downloader(DREMIO_HOST, DREMIO_PORT, dremio_token, DREMIO_QUERY, DREMIO_ENGINE)

display(dremio_dataframe)
