import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import *

# =========================================
# 1️⃣ CONFIGURATION (DOCKER + HDFS)
# =========================================

# 🚨 Network: We are inside Docker, so we talk to containers by name
KAFKA_SERVER = "kafka:29092" 
HDFS_NN = "hdfs://namenode:9000"

# 🚨 Storage: We write to HDFS, not local disk
WAREHOUSE_PATH = f"{HDFS_NN}/warehouse"
CHECKPOINT_PATH = f"{HDFS_NN}/checkpoints/flights_realtime"

# 🚨 Topic: Must match what producer.py sends
TOPIC_NAME = "flight_events" 

print(f"🔹 Kafka Server: {KAFKA_SERVER}")
print(f"🔹 Warehouse:    {WAREHOUSE_PATH}")
print(f"🔹 Checkpoint:   {CHECKPOINT_PATH}")

# =========================================
# 2️⃣ CREATE SPARK SESSION
# =========================================
spark = (
    SparkSession.builder
    .appName("IcebergStreaming_HDFS")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
    # Important: Tell Spark how to talk to HDFS
    .config("spark.hadoop.fs.defaultFS", HDFS_NN)
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3" 
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("\n✅ Spark + Iceberg + HDFS + Kafka initialized successfully")

# =========================================
# 3️⃣ READ FROM KAFKA
# =========================================
schema = StructType([
    StructField("FL_DATE", StringType()),
    StructField("OP_UNIQUE_CARRIER", StringType()),
    StructField("ORIGIN", StringType()),
    StructField("DEST", StringType()),
    StructField("DEP_DELAY", DoubleType()),
    StructField("ARR_DELAY", DoubleType()),
    StructField("CANCELLED", DoubleType())
])

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("FL_DATE", to_date(col("FL_DATE")))
)

print(f"✅ Listening to Kafka topic: {TOPIC_NAME}")

# =========================================
# 4️⃣ ENSURE ICEBERG TABLE EXISTS
# =========================================
# Note: We append to the SAME table as your batch job!
# This creates a "Lambda Architecture" (Batch + Speed layers together)
spark.sql("""
CREATE TABLE IF NOT EXISTS local.flight_stream.history_flights (
    FL_DATE DATE,
    OP_UNIQUE_CARRIER STRING,
    ORIGIN STRING,
    DEST STRING,
    DEP_DELAY DOUBLE,
    ARR_DELAY DOUBLE,
    CANCELLED DOUBLE
)
USING ICEBERG
PARTITIONED BY (years(FL_DATE))
""")

print("✅ Target Iceberg table ready: local.flight_stream.history_flights")

# =========================================
# 5️⃣ WRITE STREAM TO ICEBERG (HDFS)
# =========================================
query = (
    parsed_df
    .writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(processingTime="5 seconds") # Write a small file every 5s
    .option("checkpointLocation", CHECKPOINT_PATH)
    # We write to the SAME table name as your batch job
    .toTable("local.flight_stream.history_flights")
)

print("\n🚀 SPARK STREAMING TO HDFS STARTED")
print(f"📥 Reading from: {TOPIC_NAME}")
print("🧊 Writing to:   local.flight_stream.history_flights")
print("⏳ Waiting for planes... (Press Ctrl+C to stop)")

query.awaitTermination()