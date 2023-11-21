from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from lstm_model import lstm
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("KafkaSparkStreamingApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.home.dir", "C:\spark\hadoop") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()


kafka_bootstrap_servers = "10.100.102.189:9092"
kafka_topic = "sparkstreamtestyahoolstm"

df = ( spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load() )

# lstm_udf = udf(lstm, DoubleType())

# result_df = df.withColumn("LSTM generated value", lstm_udf(col("LSTM Value")))

query = df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
