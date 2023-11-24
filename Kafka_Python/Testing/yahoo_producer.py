import json
from kafka import KafkaProducer
import yfinance as yf

kafka_config = {
    'bootstrap_servers': ['10.0.0.137:9092'],
    'client_id': 'laila-python-producer',
    'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
    'key_serializer': lambda k: str(k).encode('utf-8') if k is not None else None
}

topic_name = 'sparkstreamtestyahooudf'

producer = KafkaProducer(**kafka_config)

yf_data = yf.download('AZN', start='2015-01-01', end='2023-05-01')
yf_data = yf_data.reset_index()
yf_data.index = range(1, len(yf_data) + 1)
yf_data = yf_data[['Date', 'Close']]

yf_data['Date'] = yf_data['Date'].apply(lambda x: int(x.timestamp()))

# Just keep the date
messages = yf_data.to_dict(orient='index')
# print(message)


for key, value in messages.items():
    value_without_date = {k: v for k, v in value.items() if k != 'Date'}
    producer.send(topic_name, key=value['Date'], value=value_without_date)

producer.close()
