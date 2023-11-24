from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from udf_functions import my_udf

python_executable_path = "C:/Python/python.exe"

spark = SparkSession.builder \
    .appName("KafkaSparkStreamingApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.home.dir", "C:\spark\hadoop") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.executorEnv.PYSPARK_PYTHON", python_executable_path) \
    .getOrCreate()

kafka_bootstrap_servers = "10.0.0.137:9092"
kafka_topic = "sparkstreamtestyahooudf"

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic)
      .load())

result_df = df.withColumn("udf_test_value", my_udf(col("value")))

query = result_df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "udf_test_value") \
    .writeStream \
    .outputMode("append") \
    .option('truncate', "false") \
    .format("console") \
    .start()

query.awaitTermination()
