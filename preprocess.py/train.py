import pandas as pd
from datetime import datetime, timedelta

# Generate synthetic data
data = {
    "PULocationID": [123, 456, 789],
    "DOLocationID": [456, 789, 123],
    "lpep_pickup_datetime": [
        "2023-10-01 08:00:00",
        "2023-10-01 12:30:00",
        "2023-10-01 19:00:00"
    ],
    "lpep_dropoff_datetime": [
        "2023-10-01 08:30:00",
        "2023-10-01 13:00:00",
        "2023-10-01 19:30:00"
    ],
    "trip_distance": [5.3, 10.1, 3.7],
    "fare_amount": [20.0, 35.5, 15.0],
    "extra": [0.5, 0.5, 0.5],
    "mta_tax": [0.3, 0.3, 0.3],
    "tip_amount": [3.0, 5.0, 2.0],
    "tolls_amount": [0.0, 0.0, 0.0],
    "ehail_fee": [0.0, 0.0, 0.0],
    "improvement_surcharge": [0.3, 0.3, 0.3],
    "total_amount": [24.1, 41.6, 17.8],
    "payment_type": [1, 1, 1],
    "trip_type": [1, 1, 1],
    "congestion_surcharge": [2.5, 2.5, 2.5]
}

# Create a DataFrame
df = pd.DataFrame(data)

# Save the synthetic dataset as 'training_data.csv'
df.to_csv('data/training_data.csv', index=False)

print("Synthetic dataset saved as 'data/training_data.csv'")
# Load the dataset
try:
    df = pd.read_csv('data/training_data.csv')  # Ensure the file exists in the 'data/' folder
except FileNotFoundError:
    raise Exception("File 'data/training_data.csv' not found. Please ensure the file exists and the path is correct.")

