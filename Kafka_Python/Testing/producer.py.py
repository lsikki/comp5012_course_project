import http.client
import json
from kafka import KafkaProducer
import time

# Kafka producer configuration
kafka_config = {
    'bootstrap_servers': ['localhost:9092', '172.17.6.120:9092'],
    'client_id': 'laila-python-producer',
    'value_serializer': lambda v: json.dumps(v).encode('utf-8')
}

# Specify the topic name
topic_microsoft = 'microsoft'
topic_amazon = 'amazon'

# Create a Kafka producer instance
producer = KafkaProducer(**kafka_config)

import requests
api_key = 'P544V2LKQW6KH46R'
symbol = 'AMZN'  # Amazon stock symbol

# Alpha Vantage endpoint for daily stock data
endpoint = 'https://www.alphavantage.co/query'
function = 'TIME_SERIES_DAILY'
output_size = 'full'
# Construct the API URL
url = f'{endpoint}?function={function}&symbol={symbol}&outputsize={output_size}&apikey={api_key}'

# Make the API request
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()

    # Access the time series data
    time_series_data = data['Time Series (Daily)']
    print(time_series_data)

    # Print the first few entries
    for date, values in list(time_series_data.items())[:5]:
        print(f'Date: {date}, Closing Price: {values["4. close"]}')
else:
    print(f'Error: {response.status_code}, {response.text}')


# Close the Kafka producer
producer.close()
