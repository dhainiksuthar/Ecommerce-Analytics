import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timedelta

from faker import Faker
from kafka import KafkaProducer

fake = Faker()
Faker.seed(42)
random.seed(42)

PAGES = [
    "/", "/home", "/search", "/product/sku-100", "/product/sku-101",
    "/product/sku-102", "/cart", "/checkout", "/category/shoes",
    "/category/electronics", "/help", "/account", "/login", "/signup"
]

EVENT_TYPES = [
    "page_view", "click", "add_to_cart", "remove_from_cart",
    "checkout_start", "purchase", "search", "impression"
]

producer = KafkaProducer(
    bootstrap_servers=['kafka:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def make_user(user_index=None):
    """Create a synthetic user profile."""
    uid = str(fake.uuid4())
    return {
        "user_id": uid,
        "anonymous": False if random.random() > 0.1 else True,
        "user_agent": fake.user_agent(),
        "country": fake.country_code(),
        "email": fake.email() if random.random() > 0.7 else None,
        "created_at": fake.date_time_between(start_date='-2y', end_date='now').isoformat()
    }

def get_stream():
    user_details = make_user()

    event = {
        "event_id": fake.uuid4(),
        "user_id": user_details.get('user_id'),
        "session_id" : str(fake.uuid4()),
        "device" : random.choice(['mobile', 'desktop']),
        "event_type" : random.choice(EVENT_TYPES),
        "page_path" : random.choice(PAGES),
        "product_id": fake.uuid4(),
        "order_id": fake.uuid4(),
        "price": round(random.uniform(5.0, 499.99)),
        "event_timestamp": (datetime.now() + timedelta(seconds=random.randint(0, 5))).isoformat()
    }

    return event

def generate_clickstream(topic_name, num_events, rate):
    cur_event = 0
    while cur_event < num_events:
        event = get_stream()
        producer.send(topic_name, event)
        cur_event = cur_event + 1
        time.sleep(rate)
    producer.flush()
    producer.close()
    return


if __name__ == "__main__":
    try:
        while True:
            generate_clickstream(
                topic_name = "ClickStream",
                num_events=100,
                rate=10
            )
    except KeyboardInterrupt:
        print("Interrupted by user", file=sys.stderr)
