# Databricks notebook source
import time
import requests
import json
from pyspark.sql.functions import col, to_timestamp, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


# COMMAND ----------

# MAGIC %run ./airport_data_schema

# COMMAND ----------

api_key = dbutils.secrets.get('testscope','aviation-key')

# COMMAND ----------

# Base URL for the API
base_url = "http://api.aviationstack.com/v1/airports"

# API key as a parameter
params = {
    'access_key': api_key,
    'limit': 100,
    'offset': 0  # Start with offset 0
}

def fetch_airport_data(params):
    all_data = []
    while True:
        try:
            # Make the API request
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Raises an error for bad responses
            
            # Get the data from the response
            data = response.json()

            # Add the fetched data to the complete list
            all_data.extend(data['data'])  # Assuming 'data' is the key in the response that holds the airport records
            
            # Check if there are more records to fetch
            if len(data['data']) < params['limit']:
                # If the number of records returned is less than the limit, we've fetched all records
                break

            # Increment the offset for the next batch
            params['offset'] += params['limit']
            
            # Wait for 1 minute before the next API call to avoid rate limiting
            print(f"Fetched {len(data['data'])} records. Waiting for 1 minute before the next call...")
            time.sleep(60)
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break
    
    return all_data

# Fetch all airport data
airport_data = fetch_airport_data(params)

print(f"Total records fetched: {len(airport_data)}")

# COMMAND ----------

rdd_airport = sc.parallelize(airport_data)
df_airports = spark.createDataFrame(rdd_airport, schema=airport_schema)

# COMMAND ----------

df_airports = df_airports.withColumn("latitude",col('latitude').cast(DoubleType())) \
.withColumn("longitude",col('longitude').cast(DoubleType())) \
.withColumn("geoname_id", col('geoname_id').cast(IntegerType()))

# COMMAND ----------

df_airports = df_airports.dropDuplicates(["iata_code","icao_code"])

# COMMAND ----------

df_airports = df_airports.filter(
    df_airports.iata_code.isNotNull() & df_airports.icao_code.isNotNull()
)

# COMMAND ----------

df_airports.show(10)

# COMMAND ----------

df_airports.write.format("delta") \
    .partitionBy("country_name") \
    .mode("overwrite") \
    .saveAsTable("airport_data_buckets")

# COMMAND ----------

# Perform Z-Ordering on iata_code and icao_code to optimize data skipping during queries
spark.sql("""
    OPTIMIZE airport_data_buckets
    ZORDER BY (iata_code, icao_code)
""")

# COMMAND ----------

spark.sql("DESCRIBE DETAIL airport_data_buckets").show(truncate=False)
