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
    'limit': 100,
    'flight_status': 'active'
}

# Airports list without spaces
airports = 'KATL'

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

all_arrivals

# COMMAND ----------

all_arrivals = [{'flight_date': '2024-09-16',
  'flight_status': 'active',
  'departure': {'airport': 'Doha International',
   'timezone': 'Asia/Qatar',
   'iata': 'DOH',
   'icao': 'OTHH',
   'terminal': None,
   'gate': None,
   'delay': 27,
   'scheduled': '2024-09-16T07:50:00+00:00',
   'estimated': '2024-09-16T07:50:00+00:00',
   'actual': '2024-09-16T08:17:00+00:00',
   'estimated_runway': '2024-09-16T08:17:00+00:00',
   'actual_runway': '2024-09-16T08:17:00+00:00'},
  'arrival': {'airport': 'Hartsfield-jackson Atlanta International',
   'timezone': 'America/New_York',
   'iata': 'ATL',
   'icao': 'KATL',
   'terminal': 'I',
   'gate': None,
   'baggage': None,
   'delay': 102,
   'scheduled': '2024-09-16T15:55:00+00:00',
   'estimated': '2024-09-16T15:55:00+00:00',
   'actual': None,
   'estimated_runway': None,
   'actual_runway': None},
  'airline': {'name': 'Qatar Airways', 'iata': 'QR', 'icao': 'QTR'},
  'flight': {'number': '755',
   'iata': 'QR755',
   'icao': 'QTR755',
   'codeshared': None},
  'aircraft': {'registration': 'A7-ANA',
   'iata': 'A35K',
   'icao': 'A35K',
   'icao24': '06A11D'},
  'live': {'updated': '2024-09-16T17:14:53+00:00',
   'latitude': 45.6905,
   'longitude': -69.7638,
   'altitude': 11582.4,
   'direction': 224.34,
   'speed_horizontal': 911.52,
   'speed_vertical': 0,
   'is_ground': False}}]

# COMMAND ----------


