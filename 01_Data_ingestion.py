# Databricks notebook source
import requests
import json
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# COMMAND ----------

api_key = dbutils.secrets.get('testscope','aviation-key')

# COMMAND ----------

# Base URL
base_url = "http://api.aviationstack.com/v1/flights"

# Common parameters
common_params = {
    'access_key': api_key,
    'limit': 100,  # Adjust as per your API plan
}

# Airports list without spaces
airports = 'KATL,KDFW,KDEN,KORD,OMDB'

# Function to fetch flights
def fetch_flights(params):
    all_flights = []
    offset = 0
    while True:
        params['offset'] = offset
        response = requests.get(base_url, params=params)
        data = response.json().get('data', [])
        if not data:
            break
        all_flights.extend(data)
        offset += params['limit']
    return all_flights

# Fetch arrivals
params_arrivals = common_params.copy()
params_arrivals['arr_icao'] = airports
all_arrivals = fetch_flights(params_arrivals)

# Fetch departures
params_departures = common_params.copy()
params_departures['dep_icao'] = airports
all_departures = fetch_flights(params_departures)

# COMMAND ----------

all_departures
