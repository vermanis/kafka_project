# kafka_project
from confluent_kafka import Producer
import json

# Producer configuration
producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)

# Simulating banking messages
messages = [
    {"account_number": "123456789", "name": "John Doe", "status": "Active", "balance": 5000},
    {"account_number": "987654321", "name": "Jane Smith", "status": "Inactive", "balance": 1200},
    {"account_number": "112233445", "name": "Alice Brown", "status": "Overdrawn", "balance": -200}
]

for message in messages:
    producer.produce('banking-details', json.dumps(message))
    producer.flush()
    print(f"Message sent: {message}")
