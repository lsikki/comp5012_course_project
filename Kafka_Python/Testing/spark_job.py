from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from datetime import datetime

def create_sequences(stock_data, length):
  sequences = []
  target_prices = []
  dates = []
  print(stock_data)

  for i in range(len(stock_data) - length):
      seq = stock_data['Close'][i:i+length]
      date = stock_data['Date'][i+length]
      target_price = stock_data['Close'][i+length]
      dates.append(date)
      sequences.append(seq)
      target_prices.append(target_price)

  return np.array(dates), np.array(sequences), np.array(target_prices)

#this is a local function that trains LSTM and outputs the predicted prices
def LSTM(df):
    dates,seq,tar = create_sequences(df, 5)
    print(dates, seq, tar)

    split = int(len(seq)*0.7)
    dates_train, dates_test, X_train, X_test, y_train, y_test = dates[:split], dates[split:], seq[:split], seq[split:], tar[:split], tar[split:]

    model = Sequential()
    model.add(LSTM(50, activation = 'relu', input_shape = (5, 1)))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mean_squared_error')

    X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], 1))
    X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], 1))
    print(y_train)

    history = model.fit(X_train, y_train, epochs=20, batch_size=32, validation_data=(X_test, y_test))    

    y_pred = model.predict(X_test)

    return y_pred

#this udf will process dates to date format
def dates_udf(key):
    date = datetime.strptime(key, "%Y-%m-%d")
    return date

#this udf format closing prices to integer type
def prices_udf(values):
    price = values.astype(int)
    return price

#registering both the udf's
reg_dates_udf = udf(dates_udf, StringType())
reg_prices_udf = udf(prices_udf, IntegerType())

spark = SparkSession.builder \
    .appName("KafkaSparkStreamingApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.home.dir", "C:\spark\hadoop") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.streaming.checkpointLocation", "file:///C:/sparkcheckpoints") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .getOrCreate()

kafka_bootstrap_servers = '172.17.12.108:9092'
kafka_topic = "amazon"

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic)
      .load())

#adding processed columns of dates and price to the dataframe
result_dates = df.withColumn("processed_dates",reg_dates_udf(col("key")))
result_closing_prices = df.withColumn("processed_closing_prices",reg_prices_udf(col("value")))

# Write the streaming DataFrame to memory so we can fetch it
query = result_closing_prices \
    .writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("streaming_data") \
    .start()

# Wait for a few seconds to collect some streaming data (you can adjust the duration)
query.awaitTermination(timeout=10)
query.stop()

#once we have the processed dataframe we shall train the LSTM locally - output is y_pred/ prediction of closing prices
#convert spark dataframe to pandas
df_pandas = spark.sql("SELECT * FROM streaming_data").toPandas()
y_pred = LSTM(df_pandas)
'''
output_path = "file:///C:/sparkoutput"  # Update the path as needed


query = df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "processed_dates") \
    .writeStream \
    .outputMode("append") \
    .option('truncate', "false") \
    .format("console") \
    .start()

query.awaitTermination()'''
