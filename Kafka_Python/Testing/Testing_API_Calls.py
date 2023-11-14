import http.client
import json
from kafka import KafkaProducer
import time

# Kafka producer configuration
kafka_config = {
    'bootstrap_servers': ['localhost:9092', '172.17.13.53:9092'],
    'client_id': 'laila-python-producer',
    'value_serializer': lambda v: json.dumps(v).encode('utf-8')
}

# Specify the topic name
topic_name = 'quickstart-events'

# Function to make API call
def make_api_call(api_key):
    conn = http.client.HTTPSConnection("api.sportradar.us")
    conn.request("GET", f"/cricket-t2/en/schedules/2023-09-30/results.xml?api_key={api_key}")
    res = conn.getresponse()
    data = res.read()
    conn.close()
    return data.decode('utf-8')  # Convert bytes to string

# Create a Kafka producer instance
producer = KafkaProducer(**kafka_config)

# Loop to make API calls with a one-second delay
for i in range(5):
    api_key = "jt3mukbuh3th6dwudfhauxg6"
    
    # Make API call
    response_data = make_api_call(api_key)
    
    # Produce the API response to Kafka with specified topic name
    kafka_message = {'api_call_index': i, 'response_data': response_data}
    producer.send(topic_name, value=kafka_message)
    
    # Introduce a one-second delay between API calls
    time.sleep(1)

# Close the Kafka producer
producer.close()
