# Basic consumer setup
from confluent_kafka import Consumer, KafkaException
import sys
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning if no offset is stored
}

consumer = Consumer(conf)

try:
    consumer.subscribe(['my_test_topic'])

    print("Starting Kafka Consumer, waiting for messages...")
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for messages with a timeout
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # End of partition event - not an error, just an indication that there are no more messages yet
                sys.stderr.write(f'%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n')
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Proper message processing
            try:
                message_value = msg.value().decode('utf-8')
                print(f"Received message: {json.loads(message_value)}")
            except json.JSONDecodeError:
                print(f"Received non-JSON message: {message_value}")

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')
except Exception as e:
    sys.stderr.write(f'%% An error occurred: {e}\n')
finally:
    # Close down consumer to commit final offsets. If you don't do this,
    # on the next run, it will start from the last committed offset.
    print("Closing Consumer.")
    consumer.close()
