import json
from kafka import KafkaProducer
import yfinance as yf
from datetime import datetime, timedelta
import requests
import csv

kafka_config = {
    'bootstrap_servers': '172.17.12.108:9092',
    'client_id': 'laila-python-producer',
    'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
    'key_serializer': lambda k: str(k).encode('utf-8') if k is not None else None
}
producer = KafkaProducer(**kafka_config)

topic_amazon = 'amazon'
api_key_amzn = 'DHKG3ADZ7GE5CE6J'
symbol_amzn = 'AMZN'
endpoint = 'https://www.alphavantage.co/query'
function = 'TIME_SERIES_DAILY'
output_size = 'full'
# Construct the API URL
url = f'{endpoint}?function={function}&symbol={symbol_amzn}&outputsize={output_size}&apikey={api_key_amzn}&datatype=csv'

# Make the API request
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    # Check the content type
    content_type = response.headers.get('content-type', '')
    if 'text/csv' in content_type:
        # Parse the CSV response
        csv_data = csv.reader(StringIO(response.text))
        
        # Display the CSV data
        for row in csv_data:
            print(row)
    else:
        print(f'Error: Response is not in CSV format. Content type: {content_type}')

else:
    print(f'Error: {response.status_code}')

producer.close()
