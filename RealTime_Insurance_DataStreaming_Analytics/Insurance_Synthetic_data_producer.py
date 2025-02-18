from confluent_kafka import Producer
import json
import time
import random
from faker import Faker

# Initialize Faker for generating sample data
fake = Faker()

KAFKA_BROKER = "pk131c-41mxj.uksouth.azure.confluent.cloud"
KAFKA_API_KEY = "RCAHBFUWDKXZwtwtwtwr3JOR"
KAFKA_API_SECRET = "vpwg0/46V6aT3QG8x5fretn5oxtHssgsgsffgsn67TbEWNjldzI8iQOsQk3+El6ie9"
TOPICS = ["Policy-details_v1", "Claims", "Customer-v1", "Payments"]

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET
}

producer = Producer(conf)

# Function to generate random policy data
def generate_policy():
    return {
        "policy_id": fake.uuid4(),
        "customer_id": fake.uuid4(),
        "policy_type": random.choice(["Auto", "Home", "Health", "Life"]),
        "premium": round(random.uniform(100, 1000), 2)
    }

# Function to generate random claim data
def generate_claim():
    return {
        "claim_id": fake.uuid4(),
        "policy_id": fake.uuid4(),
        "claim_amount": round(random.uniform(500, 5000), 2),
        "status": random.choice(["Approved", "Pending", "Denied"])
    }

# Function to generate random customer data
def generate_customer():
    return {
        "customer_id": fake.uuid4(),
        "name": fake.name(),
        "age": random.randint(18, 75),
        "city": fake.city()
    }

# Function to generate random payment data
def generate_payment():
    return {
        "payment_id": fake.uuid4(),
        "customer_id": fake.uuid4(),
        "amount": round(random.uniform(50, 5000), 2),
        "status": random.choice(["Completed", "Failed", "Pending"])
    }

# Mapping topics to their data generator functions
data_generators = {
    "Policy-details_v1": generate_policy,
    "Claims": generate_claim,
    "Customer-v1": generate_customer,
    "Payments": generate_payment
}

# Function to produce messages to Kafka
def produce_messages():
    while True:
        for topic in TOPICS:
            data = data_generators[topic]()  # Generate random data
            message = json.dumps(data)  # Convert to JSON
            producer.produce(topic, key=str(data.get("id", "")), value=message)
            print(f"✅ Sent to {topic}: {message}")

        producer.flush()  # Ensure messages are sent
        time.sleep(2)  # Wait for 2 seconds before sending more data

# Start producing messages
if __name__ == "__main__":
    print("🚀 Starting Kafka producer...")
    produce_messages()
