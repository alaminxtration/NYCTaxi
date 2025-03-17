from flask import Flask, request, jsonify
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when, unix_timestamp
from sklearn.ensemble import IsolationForest
import pandas as pd

app = Flask(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TaxiAnomalyDetection") \
    .getOrCreate()

# Train the model when the app starts
def train_model():
    try:
        # Load the dataset using PySpark
        df = spark.read.csv('data/training_data.csv', header=True, inferSchema=True)

        # Parse datetime fields
        df = df.withColumn("lpep_pickup_datetime", col("lpep_pickup_datetime").cast("timestamp")) \
               .withColumn("lpep_dropoff_datetime", col("lpep_dropoff_datetime").cast("timestamp"))

        # Derive features
        df = df.withColumn("pickup_hour", hour(col("lpep_pickup_datetime"))) \
               .withColumn("pickup_weekday", col("lpep_pickup_datetime").cast("int") % 7) \
               .withColumn("pickup_is_weekend", when((col("pickup_weekday") >= 5), 1).otherwise(0)) \
               .withColumn("trip_duration",
                           (unix_timestamp(col("lpep_dropoff_datetime")) - unix_timestamp(col("lpep_pickup_datetime"))) / 60)

        # Drop raw datetime fields
        df = df.drop("lpep_pickup_datetime", "lpep_dropoff_datetime")

        # Define features (update this list based on your dataset)
        features = ['PULocationID', 'DOLocationID',
                    'trip_distance', 'fare_amount', 'extra',
                    'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee',
                    'improvement_surcharge', 'total_amount', 'payment_type',
                    'trip_type', 'congestion_surcharge', 'pickup_hour',
                    'pickup_weekday', 'pickup_is_weekend', 'trip_duration']

        # Check for missing columns in the dataset
        missing_columns = [feature for feature in features if feature not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in dataset: {missing_columns}")

        # Add missing features with default values (if needed)
        if 'VendorID' not in df.columns:
            df = df.withColumn("VendorID", lit(1))  # Default vendor ID
        if 'RatecodeID' not in df.columns:
            df = df.withColumn("RatecodeID", lit(1))  # Default rate code
        if 'passenger_count' not in df.columns:
            df = df.withColumn("passenger_count", lit(1))  # Default passenger count

        # Update the feature list to include all required features
        features = ['VendorID', 'RatecodeID', 'PULocationID', 'DOLocationID',
                    'passenger_count', 'trip_distance', 'fare_amount', 'extra',
                    'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee',
                    'improvement_surcharge', 'total_amount', 'payment_type',
                    'trip_type', 'congestion_surcharge', 'pickup_hour',
                    'pickup_weekday', 'pickup_is_weekend', 'trip_duration']

        # Convert PySpark DataFrame to Pandas DataFrame for Scikit-Learn
        pdf = df.select(features).toPandas()

        # Train the model
        model = IsolationForest(contamination=0.01, random_state=42)
        model.fit(pdf)

        print("Model trained successfully.")
        return model
    except Exception as e:
        raise Exception(f"Error during model training: {str(e)}")

# Train the model at app startup
model = train_model()

@app.route('/predict', methods=['POST'])
def predict():
    try:
        input_data = request.json

        # Parse raw datetime fields
        pickup_datetime = datetime.strptime(input_data["lpep_pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        dropoff_datetime = datetime.strptime(input_data["lpep_dropoff_datetime"], "%Y-%m-%d %H:%M:%S")

        # Derive features
        input_data["pickup_hour"] = pickup_datetime.hour
        input_data["pickup_weekday"] = pickup_datetime.weekday()  # Monday=0, Sunday=6
        input_data["pickup_is_weekend"] = 1 if input_data["pickup_weekday"] >= 5 else 0
        input_data["trip_duration"] = (dropoff_datetime - pickup_datetime).seconds / 60

        # Remove raw datetime fields
        del input_data["lpep_pickup_datetime"]
        del input_data["lpep_dropoff_datetime"]

        # Ensure feature order matches training schema
        expected_features = ['VendorID', 'RatecodeID', 'PULocationID', 'DOLocationID',
                             'passenger_count', 'trip_distance', 'fare_amount', 'extra',
                             'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee',
                             'improvement_surcharge', 'total_amount', 'payment_type',
                             'trip_type', 'congestion_surcharge', 'pickup_hour',
                             'pickup_weekday', 'pickup_is_weekend', 'trip_duration']
        features = [input_data.get(feature, 1) for feature in expected_features]  # Default value is 1 for missing features

        # Make prediction
        prediction = model.predict([features])

        return jsonify({"prediction": prediction.tolist()})
    except KeyError as e:
        return jsonify({"error": f"Missing required field: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
