import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when, unix_timestamp, lit

# Create directories if they don't exist
os.makedirs('data', exist_ok=True)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TaxiDataPreprocessing") \
    .getOrCreate()

# Input data
input_data = {
    "VendorID": 1,
    "RatecodeID": 1,
    "PULocationID": 123,
    "DOLocationID": 456,
    "passenger_count": 2,
    "trip_distance": 5.3,
    "fare_amount": 20.0,
    "extra": 0.5,
    "mta_tax": 0.3,
    "tip_amount": 3.0,
    "tolls_amount": 0.0,
    "ehail_fee": 0.0,
    "improvement_surcharge": 0.3,
    "total_amount": 24.1,
    "payment_type": 1,
    "trip_type": 1,
    "congestion_surcharge": 2.5,
    "lpep_pickup_datetime": "2023-10-01 19:00:00",
    "lpep_dropoff_datetime": "2023-10-01 19:30:00"
}

# Convert input data into a PySpark DataFrame
columns = list(input_data.keys())
values = list(input_data.values())

# Create a single-row DataFrame
df = spark.createDataFrame([values], schema=columns)

# Parse datetime fields
df = df.withColumn("lpep_pickup_datetime", col("lpep_pickup_datetime").cast("timestamp")) \
       .withColumn("lpep_dropoff_datetime", col("lpep_dropoff_datetime").cast("timestamp"))

# Derive features
df = df.withColumn("pickup_hour", hour(col("lpep_pickup_datetime"))) \
       .withColumn("pickup_weekday", (col("lpep_pickup_datetime").cast("int") % 7)) \
       .withColumn("pickup_is_weekend", when((col("pickup_weekday") >= 5), 1).otherwise(0)) \
       .withColumn("trip_duration",
                   (unix_timestamp(col("lpep_dropoff_datetime")) - unix_timestamp(col("lpep_pickup_datetime"))) / 60)

# Drop raw datetime fields
df = df.drop("lpep_pickup_datetime", "lpep_dropoff_datetime")

# Add missing features with default values (if needed)
if 'VendorID' not in df.columns:
    df = df.withColumn("VendorID", lit(1))  # Default vendor ID
if 'RatecodeID' not in df.columns:
    df = df.withColumn("RatecodeID", lit(1))  # Default rate code
if 'passenger_count' not in df.columns:
    df = df.withColumn("passenger_count", lit(1))  # Default passenger count

# Define the final feature list
features = ['VendorID', 'RatecodeID', 'PULocationID', 'DOLocationID',
            'passenger_count', 'trip_distance', 'fare_amount', 'extra',
            'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee',
            'improvement_surcharge', 'total_amount', 'payment_type',
            'trip_type', 'congestion_surcharge', 'pickup_hour',
            'pickup_weekday', 'pickup_is_weekend', 'trip_duration']

# Select only the required features
df = df.select(features)

# Convert the PySpark DataFrame to JSON for printing
preprocessed_data = df.toJSON().collect()[0]

# Print the preprocessed data
print("Preprocessed Data:")
print(json.dumps(json.loads(preprocessed_data), indent=2))

# Stop the Spark session
spark.stop()
