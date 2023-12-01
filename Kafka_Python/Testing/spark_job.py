from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DoubleType, StructType, StructField, DateType
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from datetime import datetime

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

def LSTM_Model(spark, df):
    if len(df) == 0:
        print("DataFrame is empty.")
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

def dates_udf(key):
    key_str = key.decode('utf-8')
    date = datetime.strptime(key_str, "%Y-%m-%d")
    return key_str

def prices_udf(value):
    value_str = value.decode('utf-8')
    try:
        price = float(value_str)
        return price
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

kafka_bootstrap_servers = '10.0.0.10:9092'
kafka_topic = "amazon"

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic)
      .load())

result = df.withColumn("processed_dates", reg_dates_udf(col("key"))) \
            .withColumn("processed_closing_prices", reg_prices_udf(col("value")))


def process_batch(batch_df, batch_id):
    print(batch_df)
    pandas_df = batch_df  
    pandas_df['processed_closing_prices'] = pandas_df['processed_closing_prices'].apply(
        lambda x: float(x.decode('utf-8').replace('"', '')) if isinstance(x, bytes) else x
    )
    columns_to_keep = ['processed_dates', 'processed_closing_prices']

    pandas_filtered = pandas_df[columns_to_keep]
    
    y_pred = LSTM_Model(spark, pandas_filtered)
    print(f"Batch {batch_id} - LSTM Predictions: {y_pred}")


query = result \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination(timeout=30)
