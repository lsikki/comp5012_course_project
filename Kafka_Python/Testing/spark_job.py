from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def my_udf_function(value):
    return len(str(value))

my_udf = udf(my_udf_function, IntegerType())

spark = SparkSession.builder \
    .appName("KafkaSparkStreamingApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.home.dir", "C:\spark\hadoop") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.streaming.checkpointLocation", "file:///C:/sparkcheckpoints") \
    .getOrCreate()

kafka_bootstrap_servers = '10.0.0.137:9092,10.0.0.120:9092'
kafka_topic = "sparkstreamtestyahooudf"

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic)
      .load())

result_df = df.withColumn("udf_test_value", my_udf(col("value")))

# Specify the path to the output directory
output_path = "file:///C:/sparkoutput"  # Update the path as needed

query = result_df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "udf_test_value") \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", output_path) \
    .start()

query.awaitTermination()
