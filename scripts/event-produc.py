import json
import time
import random
from kafka import KafkaProducer
from faker import Faker

# Initialize Faker and Kafka Producer
fake = Faker()

# IMPORTANT: If running from Windows, use 'localhost:9092'
# If running inside a Docker container, use 'kafka:9092'
producer = KafkaProducer(
    bootstrap_servers=['kafka:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'ecommerce_events'

print(f"Starting to produce events to topic: {topic_name}...")


try:
    while True:
        # Create a fake ecommerce event
        event = {
            "event_id": fake.uuid4(),
            "user_id": random.randint(100, 999),
            "product_id": random.randint(1000, 5000),
            "action": random.choice(['view', 'add_to_cart', 'purchase']),
            "timestamp": fake.iso8601(),
            "price": round(random.uniform(10.0, 500.0), 2),
            "city": fake.city()
        }

        # Send to Kafka
        producer.send(topic_name, value=event)
        print(f"Sent: {event['action']} by User {event['user_id']}")
        
        # Wait a bit before sending the next one
        time.sleep(2)

except KeyboardInterrupt:
    print("Stopping the producer...")
finally:
    producer.flush()
    producer.close()