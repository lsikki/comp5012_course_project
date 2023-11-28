from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense

def my_udf_function(key,value):
    #print(key)
    #print(value)

    #convert the byte arrays to numpy array and make a dataframe
    numpy_key = np.frombuffer(key,dtype=np.uint8)
    numpy_value = np.frombuffer(value,dtype=np.uint8)
    print(numpy_key)

    #preprocess the data
    df = pd.DataFrame({'Date': numpy_key.flatten(), 'Close': numpy_value.flatten()})
    #df['Date'] = df['Date'].apply(lambda x: x.timestamp())
    df['Close'] = df['Close'].astype(int)

    #now we have date and closing price we will process sequences
    sequences = []
    target_prices = []
    dates = []

    #setting length of our seq = 5
    length = 5
    for i in range(len(df) - length):
      seq = df['Close'][i:i+length]
      date = df['Date'][i+length]
      target_price = df['Close'][i+length]
      dates.append(date)
      sequences.append(seq)
      target_prices.append(target_price)
    dates = np.array(dates)
    seq = np.array(sequences)
    tar = np.array(target_prices)

    split = int(len(seq)*0.7)
    dates_train, dates_test, X_train, X_test, y_train, y_test = dates[:split], dates[split:], seq[:split], seq[split:], tar[:split], tar[split:]

    model = Sequential()
    model.add(LSTM(50, activation = 'relu', input_shape = (5, 1)))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mean_squared_error')

    X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], 1))
    X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], 1))
    print(y_train)

    # Train the model
    history = model.fit(X_train, y_train, epochs=20, batch_size=32, validation_data=(X_test, y_test))
    y_pred = model.predict(X_test)

    return y_pred

my_udf = udf(my_udf_function, StringType())

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

result_df = df.withColumn("predicted_price",my_udf(col("key"),col("value")))
#result_df.show()
#df.withColumn("predicted_values", my_udf(spark.create_DataFrame(col("key"),col("value"))))
#df.select(col['key'],col['value'],my_udf_function)
# Specify the path to the output directory
output_path = "file:///C:/sparkoutput"  # Update the path as needed

query = result_df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "predicted_price") \
    .writeStream \
    .outputMode("append") \
    .option('truncate', "false") \
    .format("console") \
    .start()

query.awaitTermination()
