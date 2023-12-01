import json
from kafka import KafkaProducer
from io import StringIO
import requests
import csv

# Kafka configuration
kafka_config = {
    'bootstrap_servers': '10.0.0.137:9092',
    'client_id': 'python-producer',
    'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
    'key_serializer': lambda k: str(k).encode('utf-8') if k is not None else None
}
producer = KafkaProducer(**kafka_config)

# Alpha Vantage API configuration
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
    # Check the content type
    content_type = response.headers.get('content-type', '')
    if 'application/x-download' in content_type:
        # Parse the CSV response
        csv_data = csv.reader(StringIO(response.text))
        
        # Send each row to Kafka
        for row in csv_data:
            # Assuming the first column is the timestamp and the second column is the closing price
            timestamp, closing_price = row[0], row[1]
            
            # Example: Send data to Kafka
            if timestamp != 'timestamp':
                producer.send(topic_amazon, key = timestamp, value=closing_price)
            # Print
            print(timestamp)
    else:
        print(f'Error: Unexpected content type. Content type: {content_type}')

else:
    print(f'Error: {response.status_code}')

# Close the Kafka producer
producer.close()
