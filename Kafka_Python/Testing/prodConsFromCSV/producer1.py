from kafka import KafkaProducer
import csv
import time

def read_csv(file_path):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        headers = next(reader)
        for row in reader:
            yield ','.join(row)

def produce_data(producer, topic, file_path):
    for message in read_csv(file_path):
        producer.send(topic, value=message.encode())
        time.sleep(1)  # Simulate a delay between messages

if __name__ == "__main__":
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'stock_prices'
    csv_file_path = r'C:\Users\black\Desktop\comp5012_course_project\Kafka_Python\Testing\prodConsFromCSV\Amazon.csv'


    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    try:
        produce_data(producer, kafka_topic, csv_file_path)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()
