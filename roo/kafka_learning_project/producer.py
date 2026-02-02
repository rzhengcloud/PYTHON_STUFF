# Basic producer setup
from confluent_kafka import Producer
import time
import json

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

def delivery_report(err, msg):
    """
    Callback function for message delivery.
    This is called once for each message produced.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

print("Starting Kafka Producer...")
for i in range(10):
    message = {'id': i, 'content': f'Hello Kafka from Producer! {i}', 'timestamp': time.time()}
    message_str = json.dumps(message)
    producer.produce(
        'my_test_topic',
        key=str(i),
        value=message_str.encode('utf-8'), # Kafka messages are bytes
        callback=delivery_report
    )
    # Serve delivery report callbacks from previous produce() calls.
    # Also provide a timeout to ensure a quick turnaround for messages to be sent.
    producer.poll(0)
    time.sleep(1) # Simulate some work

# Wait for any outstanding messages to be delivered and delivery report callbacks to be served.
producer.flush()
print("Producer finished.")