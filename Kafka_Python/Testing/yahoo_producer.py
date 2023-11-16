import http.client
import json
from kafka import KafkaProducer
import yfinance as yf

# Kafka producer configuration
kafka_config = {
    'bootstrap_servers': ['10.100.129.65:9092'],
    'client_id': 'laila-python-producer',
    'value_serializer': lambda v: json.dumps(v).encode('utf-8')
}

# Specify the topic name
topic_name = 'sparkstreamtestyahoo'
    

# Create a Kafka producer instance
producer = KafkaProducer(**kafka_config)


yf_data = yf.download('AZN', start='2015-01-01', end='2023-05-01')
message = yf_data.to_dict(orient='records')
producer.send(topic_name, value=message)

# Close the Kafka producer
producer.close()
