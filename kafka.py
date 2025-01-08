from confluent_kafka import Consumer

# 1. Kafka Consumer Configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'banking-group',           # Consumer group ID
    'auto.offset.reset': 'earliest'        # Start reading from the beginning
}

# 2. Create the consumer
consumer = Consumer(consumer_config)

# 3. Subscribe to the topic containing banking details
consumer.subscribe(['banking-details'])

print("Listening for banking messages...")

try:
    while True:
        # 4. Poll for new messages
        msg = consumer.poll(1.0)  # Wait 1 second for a message

        if msg is None:
            continue  # No message, poll again
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # 5. Process the banking message
        message = msg.value().decode('utf-8')

        # Simulating banking message structure: JSON
        # Example message: {"account_number": "123456789", "name": "John Doe", "status": "Active", "balance": 5000}
        import json
        try:
            data = json.loads(message)
            account_number = data['account_number']
            name = data['name']
            status = data['status']
            balance = data['balance']

            print(f"Account Number: {account_number}")
            print(f"Name: {name}")
            print(f"Status: {status}")
            print(f"Balance: {balance} INR")
            print("-" * 30)

        except json.JSONDecodeError as e:
            print(f"Failed to decode message: {message}. Error: {e}")

except KeyboardInterrupt:
    print("Consumer stopped by user")

finally:
    # 6. Close the consumer
    consumer.close()

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
