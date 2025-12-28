# from kafka import KafkaProducer
# import json
# import time
# import random
# import os
# from datetime import datetime

# # 1. Check if an environment variable was passed (Highest priority)
# env_kafka = os.getenv('KAFKA_SERVER')

# if env_kafka:
#     print(f"🌐 Using Environment Variable Kafka: {env_kafka}")
#     kafka_server = env_kafka
# elif os.path.exists('/.dockerenv'):
#     print("Point of reference: 🐳 Running inside Docker container")
#     kafka_server = "kafka:29092" 
# else:
#     print("Point of reference: 💻 Running on Host Machine")
#     kafka_server = "localhost:9092"

# print(f"Connecting to Kafka at: {kafka_server}")

# print(f"Connecting to Kafka at: {kafka_server}")

# # === PRODUCER SETUP ===
# producer = KafkaProducer(
#     bootstrap_servers=kafka_server,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# airlines = ["AA", "DL", "UA", "WN"]
# airports = ["JFK", "LAX", "ATL", "SFO", "ORD"]

# TOPIC_NAME = "flight_events"

# print(f"🚀 Producer started sending to [{TOPIC_NAME}]. Press Ctrl+C to stop.")

# try:
#     while True:
#         data = {
#             "FL_DATE": str(datetime.now().date()),
#             "OP_UNIQUE_CARRIER": random.choice(airlines),
#             "ORIGIN": random.choice(airports),
#             "DEST": random.choice(airports),
#             "DEP_DELAY": round(random.uniform(-10, 120), 2),
#             "ARR_DELAY": round(random.uniform(-10, 150), 2),
#             "CANCELLED": 1.0 if random.random() < 0.05 else 0.0
#         }

#         producer.send(TOPIC_NAME, value=data)
#         print(f"Sent: {data}")

#         # Send a message every 1 second (faster than 2s for testing)
#         time.sleep(5)
# except KeyboardInterrupt:
#     print("\n🛑 Producer stopped.")
#     producer.close()

from kafka import KafkaProducer
import json
import time
import os
import csv

# === 1. KAFKA CONFIGURATION (Preserved) ===
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

# === 2. PRODUCER SETUP ===
producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC_NAME = "flight_events"
CSV_FILE = "flights_2025_06_shuffled_by_date2.csv"
INTERVAL = 5  # Seconds between each flight (adjust as needed)

# Check if file exists in current or /app directory
if not os.path.exists(CSV_FILE):
    CSV_FILE = os.path.join("/app", CSV_FILE)

# === 3. DATA REPLAY LOOP ===
print(f"🚀 Producer started replaying {CSV_FILE} to [{TOPIC_NAME}].")
print("Press Ctrl+C to stop.")

try:
    with open(CSV_FILE, mode='r', encoding='utf-8') as file:
        # DictReader automatically uses the first row as keys for the JSON
        reader = csv.DictReader(file)
        
        count = 0
        for row in reader:
            # row is a dictionary containing all 26 columns as strings
            producer.send(TOPIC_NAME, value=row)
            
            count += 1
            # Simple status log
            print(f"[{count}] Sent: {row['FL_DATE']} | {row['OP_UNIQUE_CARRIER']} {row['OP_CARRIER_FL_NUM']} | {row['ORIGIN']} -> {row['DEST']}")
            
            time.sleep(INTERVAL)

    print("🏁 Finished replaying CSV file.")

except FileNotFoundError:
    print(f"❌ Error: {CSV_FILE} not found!")
    print("👉 Make sure to: kubectl cp flights_2025_06_shuffled_by_date2.csv $SPARK_POD:/app/")
except KeyboardInterrupt:
    print("\n🛑 Producer stopped by user.")
finally:
    producer.close()