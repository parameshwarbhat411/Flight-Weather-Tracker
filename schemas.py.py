# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType,TimestampType
)

# COMMAND ----------

# Define schemas for nested structures

# Departure Schema
departure_schema = StructType([
    StructField("airport", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("terminal", StringType(), True),
    StructField("gate", StringType(), True),
    StructField("delay", IntegerType(), True),
    StructField("scheduled", TimestampType(), True),
    StructField("estimated", TimestampType(), True),
    StructField("actual", TimestampType(), True),
    StructField("estimated_runway", TimestampType(), True),
    StructField("actual_runway", TimestampType(), True)
])

# Arrival Schema
arrival_schema = StructType([
    StructField("airport", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("terminal", StringType(), True),
    StructField("gate", StringType(), True),
    StructField("baggage", StringType(), True),
    StructField("delay", IntegerType(), True),
    StructField("scheduled", TimestampType(), True),
    StructField("estimated", TimestampType(), True),
    StructField("actual", TimestampType(), True),
    StructField("estimated_runway", TimestampType(), True),
    StructField("actual_runway", TimestampType(), True)
])

# Airline Schema
airline_schema = StructType([
    StructField("name", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True)
])

codeshared_schema = StructType([
    StructField("airline_name", StringType(), True),
    StructField("airline_iata", StringType(), True),
    StructField("airline_icao", StringType(), True),
    StructField("flight_number", StringType(), True),
    StructField("flight_iata", StringType(), True),
    StructField("flight_icao", StringType(), True)
])

# Flight Schema
flight_schema = StructType([
    StructField("number", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("codeshared", codeshared_schema, True)  # Adjust if codeshared is an object
])

# Aircraft Schema
aircraft_schema = StructType([
    StructField("registration", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("icao24", StringType(), True)
])

# Live Schema
live_schema = StructType([
    StructField("updated", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("direction", DoubleType(), True),
    StructField("speed_horizontal", DoubleType(), True),
    StructField("speed_vertical", DoubleType(), True),
    StructField("is_ground", BooleanType(), True)
])

# Main Schema
main_schema = StructType([
    StructField("flight_date", DateType(), True),
    StructField("flight_status", StringType(), True),
    StructField("departure", departure_schema, True),
    StructField("arrival", arrival_schema, True),
    StructField("airline", airline_schema, True),
    StructField("flight", flight_schema, True),
    StructField("aircraft", aircraft_schema, True),
    StructField("live", live_schema, True)
])

# COMMAND ----------


