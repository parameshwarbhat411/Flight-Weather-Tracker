# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType,TimestampType
)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# Airline Schema
airline_schema = StructType([
    StructField("iataCode", StringType(), True),
    StructField("icaoCode", StringType(), True),
    StructField("name", StringType(), True)
])

# Flight Schema
flight_schema = StructType([
    StructField("iataNumber", StringType(), True),
    StructField("icaoNumber", StringType(), True),
    StructField("number", StringType(), True)
])

# Codeshared Airline Schema
codeshared_airline_schema = StructType([
    StructField("iataCode", StringType(), True),
    StructField("icaoCode", StringType(), True),
    StructField("name", StringType(), True)
])

# Codeshared Flight Schema
codeshared_flight_schema = StructType([
    StructField("iataNumber", StringType(), True),
    StructField("icaoNumber", StringType(), True),
    StructField("number", StringType(), True)
])

# Codeshared Schema
codeshared_schema = StructType([
    StructField("airline", codeshared_airline_schema, True),
    StructField("flight", codeshared_flight_schema, True)
])

# Departure Schema
departure_schema = StructType([
    StructField("actualRunway", StringType(), True),
    StructField("actualTime", StringType(), True),
    StructField("baggage", StringType(), True),
    StructField("delay", StringType(), True),
    StructField("estimatedRunway", StringType(), True),
    StructField("estimatedTime", StringType(), True),
    StructField("gate", StringType(), True),
    StructField("iataCode", StringType(), True),
    StructField("icaoCode", StringType(), True),
    StructField("scheduledTime", StringType(), True),
    StructField("terminal", StringType(), True)
])

# Arrival Schema
arrival_schema = StructType([
    StructField("actualRunway", StringType(), True),
    StructField("actualTime", StringType(), True),
    StructField("baggage", StringType(), True),
    StructField("delay", StringType(), True),
    StructField("estimatedRunway", StringType(), True),
    StructField("estimatedTime", StringType(), True),
    StructField("gate", StringType(), True),
    StructField("iataCode", StringType(), True),
    StructField("icaoCode", StringType(), True),
    StructField("scheduledTime", StringType(), True),
    StructField("terminal", StringType(), True)
])

# Main Schema
main_schema = StructType([
    StructField("airline", airline_schema, True),
    StructField("arrival", arrival_schema, True),
    StructField("codeshared", codeshared_schema, True),
    StructField("departure", departure_schema, True),
    StructField("flight", flight_schema, True),
    StructField("status", StringType(), True),
    StructField("type", StringType(), True)
])

# COMMAND ----------


