# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType,TimestampType
)

# COMMAND ----------

airport_schema = StructType([
    StructField("airport_name", StringType(), True),
    StructField("iata_code", StringType(), False),
    StructField("icao_code", StringType(), False),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("geoname_id", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("gmt", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("country_iso2", StringType(), True),
    StructField("city_iata_code", StringType(), True)
])
