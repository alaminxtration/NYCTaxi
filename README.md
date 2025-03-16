NYC Taxi Trip Data Analysis and Management

1. HDFS Setup, ETL Pipeline, and Data Structure

HDFS Setup
Objective:
To set up Hadoop Distributed File System (HDFS) for storing raw and processed NYC Taxi trip data.

Steps:
1. Install Hadoop:
   - Installed Hadoop on the cluster or local machine.  
   - Configured core-site.xml and hdfs-site.xml for HDFS settings.  

2. Create HDFS Directories:  
   - Created directories for raw and processed data:  
     hdfs dfs -mkdir /nyc_taxi_raw  
     hdfs dfs -mkdir /nyc_taxi_processed  

3. Import Raw Data: 
   - Uploaded raw data (CSV/Parquet) from the NYC TLC API and historical datasets to HDFS:  
     hdfs dfs -put local_path_to_raw_data /nyc_taxi_raw  

Tools Used: 
- Hadoop HDFS  
- Command-line interface (CLI) for HDFS operations  

ETL Pipeline
Objective:
To develop an ETL (Extract, Transform, Load) pipeline for cleaning and processing raw data, with Hive as the final storage layer.

Steps:
1. Extract:  
   - Read raw data from HDFS using PySpark:  
     yellow_taxi_df = spark.read.parquet("hdfs:///nyc_taxi_raw/yellow_taxi.parquet")  
     green_taxi_df = spark.read.parquet("hdfs:///nyc_taxi_raw/green_taxi.parquet")  
     fhv_df = spark.read.parquet("hdfs:///nyc_taxi_raw/fhv.parquet")    

2. Transform:  
   - Filtered invalid data (e.g., missing values, outliers):  
     yellow_taxi_df = yellow_taxi_df.filter(yellow_taxi_df["trip_distance"] > 0)  
     green_taxi_df = green_taxi_df.filter(green_taxi_df["trip_distance"] > 0)  
     fhv_df = fhv_df.filter(fhv_df["PUlocationID"].isNotNull())  

   - Converted coordinates into NYC map areas using GeoHash:  
     from geohash import encode  
     yellow_taxi_df = yellow_taxi_df.withColumn("geohash", encode("PULocationID", "DOLocationID"))  
     green_taxi_df = green_taxi_df.withColumn("geohash", encode("PULocationID", "DOLocationID"))  

3. Load:
   - Saved cleaned data into **Hive tables** in **CSV format** for structured storage and querying:  
     yellow_taxi_df.write.mode("overwrite").format("csv").saveAsTable("nyc_taxi.yellow_taxi_trips")  
     green_taxi_df.write.mode("overwrite").format("csv").saveAsTable("nyc_taxi.green_taxi_trips")  
     fhv_df.write.mode("overwrite").format("csv").saveAsTable("nyc_taxi.fhv_trips")  

Tools Used:
- Apache Spark (PySpark)  
- Apache Hive  
- Python (for GeoHash transformation)  

Data Structure
Objective:
To define the structure of the data stored in **Hive** for efficient querying.

Steps:
1. Hive Table Creation:  
   - Created Hive tables for structured data storage. The schema for each table is as follows:  

Yellow Taxi Schema
yellow_taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

Green Taxi Schema
green_taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", StringType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", DoubleType(), True),
    StructField("trip_type", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

FHV Schema
fhv_schema = StructType([
    StructField("dispatching_base_num", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropOff_datetime", TimestampType(), True),
    StructField("PUlocationID", DoubleType(), True),
    StructField("DOlocationID", DoubleType(), True),
    StructField("SR_Flag", StringType(), True),
    StructField("Affiliated_base_number", StringType(), True)
])

2. Partitioning:
   - Partitioned the tables by `trip_date` for faster querying:  
     PARTITIONED BY (trip_date STRING);

3. Storage Format:  
   - Stored the tables in CSV format for compatibility and ease of use:  
     STORED AS TEXTFILE;

Full Hive Table Creation Query:
CREATE TABLE yellow_taxi_trips (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    RatecodeID DOUBLE,
    store_and_fwd_flag STRING,
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE
)
PARTITIONED BY (trip_date STRING)
STORED AS TEXTFILE;

Tools Used: 
- Apache Hive  
- SQL for table creation and partitioning  

2. Visualization Setup and DeepSeek Integration

Visualization Setup
Objective:
To set up Apache Zeppelin for data visualization and exploration.

Steps:
1. Install Zeppelin:
   - Downloaded and installed Apache Zeppelin on the server or local machine.  
   - Configured Zeppelin to connect to Hive for querying cleaned data.  

2. Create Visualizations:
   - Created interactive visualizations in Zeppelin notebooks:  
     - NYC Taxi Heatmap:  
       SELECT geohash, COUNT(*) as trip_count  
       FROM yellow_taxi_trips  
       GROUP BY geohash;  
  
     - Trip Distribution:
 
       SELECT HOUR(tpep_pickup_datetime) as hour, COUNT(*) as trip_count  
       FROM yellow_taxi_trips  
       GROUP BY HOUR(tpep_pickup_datetime);  
       

Tools Used:
- Apache Zeppelin  
- Hive for querying  

DeepSeek Integration
Objective:  
To integrate the DeepSeek API for natural language queries and automated reporting.

Steps:
1. Set Up DeepSeek API:  
   - Obtained API keys from DeepSeek and installed the required Python library:  
     pip install deepseek  
 

2. Integrate with Zeppelin:  
   - Wrote Python code in Zeppelin to interact with the DeepSeek API:  
 
     import deepseek  
     deepseek.api_key = "your_api_key"  
     response = deepseek.Completion.create(  
         prompt="What is the average wait time in Queens at 7 PM?",  
         max_tokens=50  
     )  
     print(response.choices[0].text.strip())  
     ```  

3. Automated Reporting:
   - Used DeepSeek to generate natural language summaries of data trends and anomalies.  

Tools Used:  
- DeepSeek API  
- Python for API integration  

3. Machine Learning Workflows and Model Outputs

Machine Learning Workflows
Objective:  
To train machine learning models for **anomaly detection** and **demand prediction**.

Steps:
1. Data Preparation: 
   - Loaded cleaned data from Hive into PySpark:  
     yellow_taxi_df = spark.sql("SELECT * FROM nyc_taxi.yellow_taxi_trips")  
     green_taxi_df = spark.sql("SELECT * FROM nyc_taxi.green_taxi_trips")  
   - Split data into training and testing sets:  
     train_data, test_data = yellow_taxi_df.randomSplit([0.8, 0.2])  

2. Anomaly Detection: 
   - Trained a model to detect unusual trip patterns (e.g., unusually high fares or long distances): 
     from pyspark.ml.clustering import KMeans  
     kmeans = KMeans(k=2, seed=1)  
     model = kmeans.fit(train_data)  

3. Demand Prediction: 
   - Trained a regression model to predict taxi demand based on time, weather, and location:  
     from pyspark.ml.regression import LinearRegression  
     lr = LinearRegression(featuresCol="features", labelCol="trip_count")  
     model = lr.fit(train_data)  

Tools Used:
- PySpark MLlib  
- Python for model training  

Model Outputs
Objective: 
To document the results and performance of the machine learning models.

Steps:
1. Anomaly Detection Results:  
   - Identified trips flagged as anomalies (e.g., unusually high fares or long distances).  
   - Visualized anomalies using Zeppelin.  

2. Demand Prediction Results:  
   - Predicted taxi demand for different times and locations.  
   - Evaluated model performance using metrics like RMSE (Root Mean Squared Error):  
     from pyspark.ml.evaluation import RegressionEvaluator  
     evaluator = RegressionEvaluator(labelCol="trip_count", predictionCol="prediction", metricName="rmse")  
     rmse = evaluator.evaluate(predictions)  
    

Tools Used  
- PySpark MLlib  
- Zeppelin for visualization  

Conclusion
This document provides a detailed overview of the HDFS setup, ETL pipeline, data structure, visualization setup, DeepSeek integration, and machine learning workflows for the NYC Taxi Trip Data Analysis and Management project. Each section includes the steps, tools, and outputs, ensuring a comprehensive understandi
