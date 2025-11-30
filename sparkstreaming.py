import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import *

# =========================================
# 1️⃣ DYNAMIC CONFIGURATION
# =========================================

# Check if running inside Docker
IS_DOCKER = os.path.exists('/.dockerenv')

if IS_DOCKER:
    print("🐳 Running inside Docker container")
    # Inside Docker, we connect to the service name
    kafka_server = "kafka:29092"
    # Inside Docker, the path is always /app
    warehouse_path = "/app/warehouse"
    checkpoint_path = "/app/checkpoint/flights"
else:
    print("💻 Running on Host Machine (Linux/Windows)")
    # Outside Docker, we connect to localhost
    kafka_server = "localhost:9092"
    
    # Handle Windows vs Linux paths for local testing
    current_dir = os.getcwd()
    if sys.platform.startswith('win'):
        warehouse_path = "warehouse"
        # Windows-only Hadoop fix
        os.environ["HADOOP_HOME"] = "C:\\hadoop"
        os.environ["PATH"] += ";" + os.path.join(os.environ["HADOOP_HOME"], "bin")
    else:
        warehouse_path = f"file://{current_dir}/warehouse"
    
    checkpoint_path = os.path.join(current_dir, "checkpoint/flights")

print(f"🔹 Kafka Server: {kafka_server}")
print(f"🔹 Warehouse:    {warehouse_path}")
print(f"🔹 Checkpoint:   {checkpoint_path}")

# =========================================
# 2️⃣ CREATE SPARK SESSION
# =========================================
spark = (
    SparkSession.builder
    .appName("IcebergStreaming")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse_path)
    # Updated to 3.5.3 to match your container
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3" 
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("\n✅ Spark + Iceberg + Kafka initialized successfully")

# =========================================
# 3️⃣ READ FROM KAFKA
# =========================================
# Define Schema (Matches your Producer)
schema = StructType([
    StructField("FL_DATE", StringType()),
    StructField("OP_UNIQUE_CARRIER", StringType()),
    StructField("ORIGIN", StringType()),
    StructField("DEST", StringType()),
    StructField("DEP_DELAY", DoubleType()),
    StructField("ARR_DELAY", DoubleType())
])

# Read Stream
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_server)
    .option("subscribe", "flights_live")
    .option("startingOffsets", "latest") # Use "earliest" if you want to replay old messages
    .load()
)

# Parse JSON Data
parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    # Convert String Date to Actual Date Type
    .withColumn("FL_DATE", to_date(col("FL_DATE")))
)

print("✅ Kafka stream connected & JSON parsed")

# =========================================
# 4️⃣ ENSURE ICEBERG TABLE EXISTS
# =========================================
# We create the table schema if it doesn't exist yet
spark.sql("""
CREATE TABLE IF NOT EXISTS local.flight_stream.realtime_flights (
    FL_DATE DATE,
    OP_UNIQUE_CARRIER STRING,
    ORIGIN STRING,
    DEST STRING,
    DEP_DELAY DOUBLE,
    ARR_DELAY DOUBLE
)
USING ICEBERG
""")

print("✅ Iceberg table ready: local.flight_stream.realtime_flights")

# =========================================
# 5️⃣ WRITE STREAM TO ICEBERG
# =========================================
# query = (
#     parsed_df
#     .writeStream
#     .format("iceberg")  # Explicitly state the format
#     .outputMode("append")
#     .option("checkpointLocation", checkpoint_path)
#     .toTable("local.flight_stream.realtime_flights")
# )

query = (
    parsed_df
    .writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(processingTime="5 seconds")  # <--- Add this Trigger
    .option("checkpointLocation", checkpoint_path)
    .toTable("local.flight_stream.realtime_flights")
)

print("\n🚀 SPARK STREAMING TO ICEBERG STARTED")
print("📥 Reading from Kafka topic: flights_live")
print("🧊 Writing to Iceberg table: local.flight_stream.realtime_flights")
print("⏳ Waiting for data... (Press Ctrl+C to stop)")

query.awaitTermination()