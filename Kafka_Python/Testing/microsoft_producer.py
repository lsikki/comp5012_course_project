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

topic_microsoft = 'microsoft'
api_key_msft = 'DHKG3ADZ7GE5CE6J'
symbol_msft = 'MSFT'
endpoint = 'https://www.alphavantage.co/query'
function = 'TIME_SERIES_DAILY'
output_size = 'full'
url = f'{endpoint}?function={function}&symbol={symbol_msft}&outputsize={output_size}&apikey={api_key_msft}'
# Make the API request
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    print(data)
    # Access the time series data
    time_series_data = data.get('Time Series (Daily)', {})
    
    # Filter out entries with invalid closing prices
    valid_entries = []
    for date, values in time_series_data.items():
        try:
            closing_price = float(values['4. close'])
            valid_entries.append((date, closing_price))
        except ValueError:
            print(f"Skipping entry with invalid closing price for date {date}: {values['4. close']}")

    if valid_entries:
        # Sending the filtered entries
        for date, closing_price in valid_entries:
            print(f'Date: {date}, Closing Price: {closing_price}')
            producer.send(topic_microsoft, key=date, value=closing_price)
    else:
        print("No valid entries to send to Kafka.")
else:
    print(f'Error: {response.status_code}, {response.text}')

producer.close()