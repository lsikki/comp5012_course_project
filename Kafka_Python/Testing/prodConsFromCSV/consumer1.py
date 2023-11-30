from confluent_kafka import Consumer, KafkaError

def consume_data(consumer, topic):
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event, not an error
                    continue
                else:
                    print(msg.error())
                    break

            # Process the received message
            print('Received message: {}'.format(msg.value().decode('utf-8')))

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'stock_prices'

    consumer_config = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'  # Change to 'latest' if you want to read only new messages
    }

    consumer = Consumer(consumer_config)

    try:
        consume_data(consumer, kafka_topic)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
