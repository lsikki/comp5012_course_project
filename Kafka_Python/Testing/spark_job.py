import json
import pandas as pd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from datetime import datetime

# Function to create sequences for LSTM
def create_sequences(stock_data, length):
    sequences = []
    target_prices = []
    dates = []

    for i in range(len(stock_data) - length):
        seq = stock_data['Close'][i:i+length]
        date = stock_data['Date'][i+length]
        target_price = stock_data['Close'][i+length]
        dates.append(date)
        sequences.append(seq)
        target_prices.append(target_price)

    return np.array(dates), np.array(sequences), np.array(target_prices)

# LSTM Model
def LSTM_Model(spark, df):
    if len(df) == 0:
        logger.info("DataFrame is empty.")
        return

    dates, seq, tar = create_sequences(df, 5)

    split = int(len(seq) * 0.7)
    dates_train, dates_test, X_train, X_test, y_train, y_test = (
        dates[:split],
        dates[split:],
        seq[:split],
        seq[split:],
        tar[:split],
        tar[split:],
    )

    model = Sequential()
    model.add(LSTM(50, activation='relu', input_shape=(5, 1)))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mean_squared_error', run_eagerly=True)  

    X_train = np.expand_dims(X_train, axis=1)
    X_test = np.expand_dims(X_test, axis=1)

    X_train_df = spark.createDataFrame([(a.tolist(), b.tolist()) for a, b in zip(X_train, y_train)], ["features", "label"])
    X_test_df = spark.createDataFrame([(a.tolist(), b.tolist()) for a, b in zip(X_test, y_test)], ["features", "label"])

    history = model.fit(X_train_df, epochs=20, batch_size=32, validation_data=X_test_df)
    y_pred = model.predict(X_test)

    return y_pred

# UDF to process dates
def dates_udf(key):
    key_str = key.decode('utf-8')
    date = datetime.strptime(key_str, "%Y-%m-%d")
    return date

# UDF to process prices
def prices_udf(value):
    value_str = value.decode('utf-8').replace('"', '').strip()
    
    try:
        price = int(value_str)
        return price
    except ValueError:
        return None  

# UDF registration
reg_dates_udf = udf(dates_udf, DateType())
reg_prices_udf = udf(prices_udf, IntegerType())

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Spark Session creation
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.home.dir", "C:\spark\hadoop") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.streaming.checkpointLocation", "file:///C:/sparkcheckpoints") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .getOrCreate()

# Kafka setup
kafka_bootstrap_servers = '10.0.0.137:9092'
kafka_topic = "amazon"

# Read data from Kafka
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic)
      .load())

df.printSchema()

# Process data with UDFs
result = df.withColumn("processed_dates", reg_dates_udf(col("key"))) \
            .withColumn("processed_closing_prices", reg_prices_udf(col("value")))

# Function to process each batch
def process_batch(batch_df, batch_id):
    pandas_df = batch_df.toPandas()

    if len(pandas_df) == 0:
        logger.info(f"Batch {batch_id} is empty.")
        return

    try:
        # Extracting date and closing prices from the batch
        date = pandas_df['processed_dates'].iloc[-1]
        closing_prices_str = pandas_df['processed_closing_prices'].iloc[-1]

        logger.info(f"Processing Batch {batch_id} - Date: {date}, Closing Prices: {closing_prices_str}")

        # Handling different scenarios for closing prices content
        if not pd.isna(closing_prices_str) and closing_prices_str.lower() != 'nan':
            # If it's a non-null string and not 'nan', attempt to convert it to a JSON object
            closing_prices = json.loads(closing_prices_str)
        else:
            # If it's null, NaN, or 'nan', handle accordingly (e.g., raise an error or set to an empty list)
            closing_prices = []

        # Create a DataFrame with 'Date' and 'Close' columns
        batch_df = pd.DataFrame({'Date': [date] * len(closing_prices), 'Close': closing_prices})

        logger.info(f"Processed Batch {batch_id} - Data: {batch_df.to_dict()}")

        # Perform LSTM predictions on the batch_df
        y_pred = LSTM_Model(spark, batch_df)
        if y_pred is not None:
            logger.info(f"Batch {batch_id} - LSTM Predictions: {y_pred}")
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")

# Start the streaming query
query = result \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination(timeout=30)
