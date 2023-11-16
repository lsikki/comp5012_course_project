from pyspark.sql import SparkSession
from kafka import KafkaProducer
import http.client
import json
import time

spark = SparkSession.builder.appName("PySparkKafkaProducer").getOrCreate()

kafka_config = {
    'bootstrap_servers': ['10.0.0.137:9092'],
    'client_id': 'laila-pyspark-producer',
    'value_serializer': lambda v: json.dumps(v).encode('utf-8')
}

topic_name = 'sparktest'

def make_api_call(api_key):
    conn = http.client.HTTPSConnection("api.sportradar.us")
    conn.request("GET", f"/cricket-t2/en/schedules/2023-09-30/results.xml?api_key={api_key}")
    res = conn.getresponse()
    data = res.read()
    conn.close()
    return data.decode('utf-8') 

producer = KafkaProducer(**kafka_config)

for i in range(5):
    api_key = "jt3mukbuh3th6dwudfhauxg6"
    response_data = make_api_call(api_key)
    kafka_message = {'api_call_index': i, 'response_data': response_data}
    producer.send(topic_name, value=kafka_message)
    time.sleep(1)

producer.close()

spark.stop()
