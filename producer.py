# from kafka import KafkaProducer
# import json
# import time
# import random
# from datetime import datetime

# producer = KafkaProducer(
#     bootstrap_servers="localhost:9092",
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# airlines = ["AA", "DL", "UA", "WN"]
# airports = ["JFK", "LAX", "ATL", "SFO", "ORD"]

# while True:
#     data = {
#         "FL_DATE": str(datetime.now().date()),
#         "OP_UNIQUE_CARRIER": random.choice(airlines),
#         "ORIGIN": random.choice(airports),
#         "DEST": random.choice(airports),
#         "DEP_DELAY": round(random.uniform(-10, 120), 2),
#         "ARR_DELAY": round(random.uniform(-10, 150), 2)
#     }

#     producer.send("flights_live", value=data)
#     print("Sent:", data)

#     time.sleep(2)

from kafka import KafkaProducer
import json
import time
import random
import os
from datetime import datetime

# === DYNAMIC CONFIGURATION ===
# Check if running inside Docker by looking for the .dockerenv file
if os.path.exists('/.dockerenv'):
    print("🐳 Running inside Docker container")
    # Inside Docker, we talk to the service name "kafka" on the internal port
    kafka_server = "kafka:29092"
else:
    print("💻 Running on Host Machine")
    # Outside Docker, we talk to localhost on the external port
    kafka_server = "localhost:9092"

print(f"Connecting to Kafka at: {kafka_server}")

# === PRODUCER SETUP ===
producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

airlines = ["AA", "DL", "UA", "WN"]
airports = ["JFK", "LAX", "ATL", "SFO", "ORD"]

print("🚀 Producer started. Press Ctrl+C to stop.")

try:
    while True:
        data = {
            "FL_DATE": str(datetime.now().date()),
            "OP_UNIQUE_CARRIER": random.choice(airlines),
            "ORIGIN": random.choice(airports),
            "DEST": random.choice(airports),
            "DEP_DELAY": round(random.uniform(-10, 120), 2),
            "ARR_DELAY": round(random.uniform(-10, 150), 2),
            # ADD THIS: 5% chance of being cancelled (1.0), otherwise 0.0
            "CANCELLED": 1.0 if random.random() < 0.05 else 0.0
        }

        producer.send("flights_live", value=data)
        print(f"Sent: {data}")

        time.sleep(2)
except KeyboardInterrupt:
    print("\n🛑 Producer stopped.")
    producer.close()