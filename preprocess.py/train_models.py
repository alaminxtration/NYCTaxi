import pandas as pd
from sklearn.ensemble import IsolationForest
from xgboost import XGBRegressor
import joblib

# Function to preprocess datetime columns
def preprocess_datetime(df):
    # Check if datetime columns exist
    if 'lpep_pickup_datetime' in df.columns and 'lpep_dropoff_datetime' in df.columns:
        # Convert datetime columns to numerical features
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

        df['pickup_hour'] = df['lpep_pickup_datetime'].dt.hour
        df['pickup_weekday'] = df['lpep_pickup_datetime'].dt.weekday
        df['pickup_is_weekend'] = (df['lpep_pickup_datetime'].dt.weekday // 5).astype(int)
        df['trip_duration'] = (df['lpep_dropoff_datetime'] - df['lpep_pickup_datetime']).dt.total_seconds() / 60

        # Drop original datetime columns
        df = df.drop(columns=['lpep_pickup_datetime', 'lpep_dropoff_datetime'])
    else:
        raise KeyError("Missing required datetime columns: 'lpep_pickup_datetime' or 'lpep_dropoff_datetime'")
    return df

# Function to clean non-numeric columns
def clean_non_numeric_columns(df):
    for col in df.columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            print(f"Cleaning non-numeric column: {col}")
            # Replace 'Y'/'N' with 1/0
            if df[col].isin(['Y', 'N']).any():
                df[col] = df[col].map({'Y': 1, 'N': 0})
            else:
                # Convert to numeric, invalid values become NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0)  # Replace NaN with 0
    return df

# Load training data
X_train = pd.read_csv('data/X_train.csv')
y_train = pd.read_csv('data/y_train.csv')

# Preprocess datetime columns
X_train = preprocess_datetime(X_train)

# Clean non-numeric columns
X_train = clean_non_numeric_columns(X_train)

# --- Train Anomaly Detection Model ---
print("Training anomaly detection model...")
model_anomaly = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
model_anomaly.fit(X_train)

# Save the anomaly detection model
joblib.dump(model_anomaly, 'models/anomaly_model.pkl')
print("Anomaly detection model trained and saved!")

# --- Train Demand Prediction Model ---
print("Training demand prediction model...")
model_demand = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
model_demand.fit(X_train, y_train)

# Save the demand prediction model
joblib.dump(model_demand, 'models/demand_model.pkl')
print("Demand prediction model trained and saved!")
