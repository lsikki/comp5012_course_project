import http.client
import json
from kafka import KafkaProducer
import yfinance as yf

kafka_config = {
    'bootstrap_servers': ['10.100.102.189:9092'],
    'client_id': 'laila-python-producer',
    'value_serializer': lambda v: json.dumps(v).encode('utf-8')
}

topic_name = 'sparkstreamtestyahoolstm'
    

producer = KafkaProducer(**kafka_config)

yf_data = yf.download('AZN', start='2015-01-01', end='2023-05-01')
message = yf_data.to_dict(orient='records')
producer.send(topic_name, value=message)

producer.close()
