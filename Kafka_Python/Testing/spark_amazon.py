from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, DateType
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from datetime import datetime
from sklearn.metrics import mean_squared_error
import pandas as pd

def create_sequences(stock_data, length):
    sequences = []
    target_prices = []
    dates = []
    print(stock_data)

    for i in range(len(stock_data) - length):
        seq = stock_data['processed_closing_prices'][i:i+length]
        date = stock_data['processed_dates'][i+length]
        target_price = stock_data['processed_closing_prices'][i+length]
        
        # Log the 'Close' values here
        print(f"Date: {date}, Close: {seq}")

        dates.append(date)
        sequences.append(seq)
        target_prices.append(target_price)

    return np.array(dates), np.array(sequences), np.array(target_prices)


# LSTM Model
def LSTM_Model(pandas_df, df):
    if len(df) != 0:
        print("printing length: " + str(len(df)))
        #print("the whole dataframe")
        #print(pandas_df)
        dates,seq,tar = create_sequences(df, 5)
        
        #print(dates, seq, tar)

        split = int(len(seq)*0.7)
        dates_train, dates_test, X_train, X_test, y_train, y_test = dates[:split], dates[split:], seq[:split], seq[split:], tar[:split], tar[split:]

        model = Sequential()
        model.add(LSTM(50, activation = 'relu', input_shape = (5, 1)))
        model.add(Dense(1))
        model.compile(optimizer='adam', loss='mean_squared_error')

        X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], 1))
        X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], 1))
        #print(y_train)

        history = model.fit(X_train, y_train, epochs=20, batch_size=32, validation_data=(X_test, y_test))    

        import matplotlib.pyplot as plt
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        print(mse)
        
        plt.plot(dates_test, y_pred)
        plt.plot(dates_test, y_test)
        plt.xlabel('Years')
        plt.ylabel('Stock Price')
        plt.title('Amazon')
        plt.legend()
        plt.savefig('Amazon_Stock_Prediction.png')
        plt.show()
    else:
        y_pred = []
    return y_pred

def dates_udf(key):
    key_str = key.decode('utf-8')
    date = datetime.strptime(key_str, "%Y-%m-%d")
    return date

# UDF to process prices
def prices_udf(value):
    value_str = value.decode('utf-8')
    x = value_str
    try:
        return x
    except ValueError:
        return None

reg_dates_udf = udf(dates_udf, DateType())
reg_prices_udf = udf(prices_udf, DoubleType())

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

result = df.withColumn("processed_dates", reg_dates_udf(col("key"))) \
            .withColumn("processed_closing_prices", col("value"))


def process_batch(batch_df, batch_id):
    pandas_df = batch_df.toPandas()
        
    pandas_df['processed_closing_prices'] = pandas_df['processed_closing_prices'].apply(
        lambda x: float(x.decode('utf-8').replace('"', '')) if isinstance(x, bytes) else x
    )
    columns_to_keep = ['processed_dates', 'processed_closing_prices']

    pandas_filtered = pandas_df[columns_to_keep]
    print('LAILAAAAAAAAAAAAAA', pandas_filtered)
    
    y_pred = LSTM_Model(pandas_df, pandas_filtered)
    print(f"Batch {batch_id} - LSTM Predictions: {y_pred}")


query = result \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .trigger(processingTime='15 seconds') \
    .start()

query.awaitTermination(timeout=30)
