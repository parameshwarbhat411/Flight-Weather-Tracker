# Databricks notebook source
import time
import requests
import json
from pyspark.sql.functions import col, to_timestamp, lit, when, concat, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


# COMMAND ----------

# MAGIC %run ./schemas

# COMMAND ----------

api_key = dbutils.secrets.get('testscope','aviation-key')

# COMMAND ----------

# Base URL
base_url = "http://api.aviationstack.com/v1/timetable"

# Common parameters (excluding 'type')
common_params = {
    'access_key': api_key,
    'limit': 100,         # Maximum number of records per API call
    'iataCode': 'ATL',    # IATA code for Atlanta airport
    'icaoCode': 'KATL'    # ICAO code for Atlanta airport
}

# Function to fetch flights
def fetch_flights(params):
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        data = response.json().get('data', [])
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return []

# Fetch arrivals
params_arrivals = common_params.copy()
params_arrivals['type'] = 'arrival'

print("Fetching arrival data...")
arrivals_data = fetch_flights(params_arrivals)
print(f"Fetched {len(arrivals_data)} arrival records.")

# Wait for 1 minute before the next API call to comply with rate limits
print("Waiting for 1 minute to comply with API rate limits...")
time.sleep(60)

# Fetch departures
params_departures = common_params.copy()
params_departures['type'] = 'departure'

print("Fetching departure data...")
departures_data = fetch_flights(params_departures)
print(f"Fetched {len(departures_data)} departure records.")

# Now you have 'arrivals_data' and 'departures_data' to process or store as needed

# COMMAND ----------

departures_data

# COMMAND ----------

rdd_arrival = sc.parallelize(arrivals_data)
rdd_departure = sc.parallelize(departures_data)
df_arrival = spark.createDataFrame(rdd_arrival, schema= main_schema)
df_departure = spark.createDataFrame(rdd_departure, schema= main_schema)

# COMMAND ----------

df_departure_flat = df_departure.select(
    # Departure fields
    col("departure.actualRunway").alias("departure_actualRunway"),
    col("departure.actualTime").alias("departure_actualTime"),
    col("departure.baggage").alias("departure_baggage"),
    col("departure.delay").alias("departure_delay"),
    col("departure.estimatedRunway").alias("departure_estimatedRunway"),
    col("departure.estimatedTime").alias("departure_estimatedTime"),
    col("departure.gate").alias("departure_gate"),
    col("departure.iataCode").alias("departure_iataCode"),
    col("departure.icaoCode").alias("departure_icaoCode"),
    col("departure.scheduledTime").alias("departure_scheduledTime"),
    col("departure.terminal").alias("departure_terminal"),
    # Arrival fields
    col("arrival.actualRunway").alias("arrival_actualRunway"),
    col("arrival.actualTime").alias("arrival_actualTime"),
    col("arrival.baggage").alias("arrival_baggage"),
    col("arrival.delay").alias("arrival_delay"),
    col("arrival.estimatedRunway").alias("arrival_estimatedRunway"),
    col("arrival.estimatedTime").alias("arrival_estimatedTime"),
    col("arrival.gate").alias("arrival_gate"),
    col("arrival.iataCode").alias("arrival_iataCode"),
    col("arrival.icaoCode").alias("arrival_icaoCode"),
    col("arrival.scheduledTime").alias("arrival_scheduledTime"),
    col("arrival.terminal").alias("arrival_terminal"),
    # Airline fields
    col("airline.name").alias("airline_name"),
    col("airline.iataCode").alias("airline_iataCode"),
    col("airline.icaoCode").alias("airline_icaoCode"),
    # Flight fields
    col("flight.number").alias("flight_number"),
    col("flight.iataNumber").alias("flight_iataNumber"),
    col("flight.icaoNumber").alias("flight_icaoNumber"),
    # Codeshared fields
    col("codeshared.airline.name").alias("codeshared_airline_name"),
    col("codeshared.airline.iataCode").alias("codeshared_airline_iataCode"),
    col("codeshared.airline.icaoCode").alias("codeshared_airline_icaoCode"),
    col("codeshared.flight.number").alias("codeshared_flight_number"),
    col("codeshared.flight.iataNumber").alias("codeshared_flight_iataNumber"),
    col("codeshared.flight.icaoNumber").alias("codeshared_flight_icaoNumber"),
    # Status and Type fields
    col("status").alias("flight_status"),
    col("type").alias("flight_type")
)

# COMMAND ----------

df_arrival_flat = df_arrival.select(
    # Departure fields
    col("departure.actualRunway").alias("departure_actualRunway"),
    col("departure.actualTime").alias("departure_actualTime"),
    col("departure.baggage").alias("departure_baggage"),
    col("departure.delay").alias("departure_delay"),
    col("departure.estimatedRunway").alias("departure_estimatedRunway"),
    col("departure.estimatedTime").alias("departure_estimatedTime"),
    col("departure.gate").alias("departure_gate"),
    col("departure.iataCode").alias("departure_iataCode"),
    col("departure.icaoCode").alias("departure_icaoCode"),
    col("departure.scheduledTime").alias("departure_scheduledTime"),
    col("departure.terminal").alias("departure_terminal"),
    # Arrival fields
    col("arrival.actualRunway").alias("arrival_actualRunway"),
    col("arrival.actualTime").alias("arrival_actualTime"),
    col("arrival.baggage").alias("arrival_baggage"),
    col("arrival.delay").alias("arrival_delay"),
    col("arrival.estimatedRunway").alias("arrival_estimatedRunway"),
    col("arrival.estimatedTime").alias("arrival_estimatedTime"),
    col("arrival.gate").alias("arrival_gate"),
    col("arrival.iataCode").alias("arrival_iataCode"),
    col("arrival.icaoCode").alias("arrival_icaoCode"),
    col("arrival.scheduledTime").alias("arrival_scheduledTime"),
    col("arrival.terminal").alias("arrival_terminal"),
    # Airline fields
    col("airline.name").alias("airline_name"),
    col("airline.iataCode").alias("airline_iataCode"),
    col("airline.icaoCode").alias("airline_icaoCode"),
    # Flight fields
    col("flight.number").alias("flight_number"),
    col("flight.iataNumber").alias("flight_iataNumber"),
    col("flight.icaoNumber").alias("flight_icaoNumber"),
    # Codeshared fields
    col("codeshared.airline.name").alias("codeshared_airline_name"),
    col("codeshared.airline.iataCode").alias("codeshared_airline_iataCode"),
    col("codeshared.airline.icaoCode").alias("codeshared_airline_icaoCode"),
    col("codeshared.flight.number").alias("codeshared_flight_number"),
    col("codeshared.flight.iataNumber").alias("codeshared_flight_iataNumber"),
    col("codeshared.flight.icaoNumber").alias("codeshared_flight_icaoNumber"),
    # Status and Type fields
    col("status").alias("flight_status"),
    col("type").alias("flight_type")
)

# COMMAND ----------

df_arrival_flat.show(10)

# COMMAND ----------

df_arrival_flat = df_arrival_flat.withColumnRenamed('type', 'flight_type')
df_departure_flat = df_departure_flat.withColumnRenamed('type', 'flight_type')
df_combined = df_arrival_flat.unionByName(df_departure_flat)

# COMMAND ----------

df_combined.where(col('flight_type') == 'departure').show(10)

# COMMAND ----------

df_combined = df_combined.withColumn("departure_delay", when(col("departure_delay").isNotNull(), col("departure_delay").cast(IntegerType())).otherwise(None)).withColumn("arrival_delay", when(col("arrival_delay").isNotNull(), col("arrival_delay").cast(IntegerType())).otherwise(None))

# COMMAND ----------

# Define the timestamp format based on your data
timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS"

# List of time fields to convert
time_fields = [
    "departure_scheduledTime",
    "departure_estimatedTime",
    "departure_actualTime",
    "departure_estimatedRunway",
    "departure_actualRunway",
    "arrival_scheduledTime",
    "arrival_estimatedTime",
    "arrival_actualTime",
    "arrival_estimatedRunway",
    "arrival_actualRunway"
]

# Convert time fields from string to timestamp
for field in time_fields:
    df_combined = df_combined.withColumn(
        field, to_timestamp(col(field), timestamp_format)
    )

# COMMAND ----------

df_combined.printSchema()

# COMMAND ----------

df_combined.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Cleaning and Validation

# COMMAND ----------

df_combined = df_combined.fillna({
    'departure_delay': 0,
    'arrival_delay': 0
})

df_combined = df_combined.dropna(subset=['flight_number','departure_iataCode','arrival_iataCode'])

# COMMAND ----------

df_combined = df_combined.withColumn('flight_date', col('departure_scheduledTime').cast('date'))
df_combined = df_combined.withColumn('display_airline',
                when(
                        col('codeshared_airline_name').isNotNull(),
                        concat(
                            col('airline_name'),
                            lit('(Codeshare: '),
                            col('codeshared_airline_name'),
                            lit(')')
                        )
                    ).otherwise(col('airline_name'))
            )
df_combined = df_combined.dropDuplicates(['flight_number','flight_date'])
df_combined = df_combined.filter((col('departure_delay') >= 0) & (col('arrival_delay') >= 0))
df_combined = df_combined.withColumn('total_delay', col('departure_delay') + col('arrival_delay'))

# COMMAND ----------

df_combined = df_combined.withColumn('on_time', (col('total_delay') == 0))
df_combined = df_combined.withColumn('departure_scheduled_time_utc',from_utc_timestamp(col('departure_scheduledTime'),'UTC')).withColumn('arrival_scheduled_time_utc',from_utc_timestamp(col('arrival_scheduledTime'),'UTC')).withColumn('departure_actual_time_utc',from_utc_timestamp(col('departure_actualTime'),'UTC')).withColumn('arrival_actual_time_utc',from_utc_timestamp(col('arrival_actualTime'),'UTC')).withColumn('departure_estimated_time_utc',from_utc_timestamp(col('departure_estimatedTime'),'UTC')).withColumn('arrival_estimated_time_utc',from_utc_timestamp(col('arrival_estimatedTime'),'UTC'))

# COMMAND ----------

display(df_combined.select('flight_status'))
