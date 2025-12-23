from kafka import KafkaProducer
import json
import time
import random
import os
from datetime import datetime

# 1. Check if an environment variable was passed (Highest priority)
env_kafka = os.getenv('KAFKA_SERVER')

if env_kafka:
    print(f"🌐 Using Environment Variable Kafka: {env_kafka}")
    kafka_server = env_kafka
elif os.path.exists('/.dockerenv'):
    print("Point of reference: 🐳 Running inside Docker container")
    kafka_server = "kafka:29092" 
else:
    print("Point of reference: 💻 Running on Host Machine")
    kafka_server = "localhost:9092"

print(f"Connecting to Kafka at: {kafka_server}")

print(f"Connecting to Kafka at: {kafka_server}")

# === PRODUCER SETUP ===
producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

airlines = ["AA", "DL", "UA", "WN"]
airports = ["JFK", "LAX", "ATL", "SFO", "ORD"]

TOPIC_NAME = "flight_events"

print(f"🚀 Producer started sending to [{TOPIC_NAME}]. Press Ctrl+C to stop.")

try:
    while True:
        data = {
            "FL_DATE": str(datetime.now().date()),
            "OP_UNIQUE_CARRIER": random.choice(airlines),
            "ORIGIN": random.choice(airports),
            "DEST": random.choice(airports),
            "DEP_DELAY": round(random.uniform(-10, 120), 2),
            "ARR_DELAY": round(random.uniform(-10, 150), 2),
            "CANCELLED": 1.0 if random.random() < 0.05 else 0.0
        }

        producer.send(TOPIC_NAME, value=data)
        print(f"Sent: {data}")

        # Send a message every 1 second (faster than 2s for testing)
        time.sleep(5)
except KeyboardInterrupt:
    print("\n🛑 Producer stopped.")
    producer.close()