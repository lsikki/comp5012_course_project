import json
from kafka import KafkaProducer
import yfinance as yf
from datetime import datetime, timedelta
import requests

kafka_config = {
    'bootstrap_servers': '172.17.12.108:9092',
    'client_id': 'laila-python-producer',
    'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
    'key_serializer': lambda k: str(k).encode('utf-8') if k is not None else None
}
producer = KafkaProducer(**kafka_config)

#topic_name = 'sparkstreamtestyahooudf'
topic_microsoft = 'microsoft'

topic_amazon = 'amazon'
api_key_amzn = 'P544V2LKQW6KH46R'
symbol_amzn = 'AMZN'
endpoint = 'https://www.alphavantage.co/query'
function = 'TIME_SERIES_DAILY'
output_size = 'full'
# Construct the API URL
url = f'{endpoint}?function={function}&symbol={symbol_amzn}&outputsize={output_size}&apikey={api_key_amzn}'

# Make the API request
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()

    # Access the time series data
    time_series_data = data['Time Series (Daily)']
    #print(time_series_data)
    
    # Sending the first few entries for now
    for date, values in list(time_series_data.items()):
        print(f'Date: {date}, Closing Price: {values["4. close"]}')
        producer.send(topic_amazon, key=date, value=values["4. close"])
else:
    print(f'Error: {response.status_code}, {response.text}')


'''

yf_data = yf.download('AZN', start='2023-01-01', end='2023-02-01')
yf_data = yf_data.reset_index()
yf_data.index = range(1, len(yf_data) + 1)
yf_data = yf_data[['Date', 'Close']]

yf_data['Date'] = yf_data['Date'].apply(lambda x: int(x.timestamp()))

# Just keep the date
messages = yf_data.to_dict(orient='index')

for key, value in messages.items():
    value_without_date = {k: v for k, v in value.items() if k != 'Date'}
    producer.send(topic_name, key=value['Date'], value=value_without_date)
'''

producer.close()
