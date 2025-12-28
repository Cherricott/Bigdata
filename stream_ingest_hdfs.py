import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import *

# =========================================
# 1️⃣ CONFIGURATION
# =========================================
KAFKA_SERVER = "kafka:29092" 
HDFS_NN = "hdfs://namenode:9000"
WAREHOUSE_PATH = f"{HDFS_NN}/warehouse"
CHECKPOINT_PATH = f"{HDFS_NN}/checkpoints/flights_stream"
TOPIC_NAME = "flight_events" 

# =========================================
# 2️⃣ CREATE SPARK SESSION
# =========================================
spark = (
    SparkSession.builder
    .appName("FlightIngest_26Cols")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
    .config("spark.hadoop.fs.defaultFS", HDFS_NN)
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" 
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================================
# 3️⃣ SCHEMA DEFINITION (26 COLUMNS)
# =========================================
# Note: StructField names MUST match the CSV headers exactly
schema = StructType([
    StructField("FL_DATE", StringType()),
    StructField("OP_UNIQUE_CARRIER", StringType()),
    StructField("TAIL_NUM", StringType()),
    StructField("OP_CARRIER_FL_NUM", StringType()),
    StructField("ORIGIN", StringType()),
    StructField("ORIGIN_CITY_NAME", StringType()),
    StructField("DEST", StringType()),
    StructField("DEST_CITY_NAME", StringType()),
    StructField("DEP_TIME", DoubleType()),
    StructField("DEP_DELAY", DoubleType()),
    StructField("TAXI_OUT", DoubleType()),
    StructField("WHEELS_OFF", DoubleType()),
    StructField("WHEELS_ON", DoubleType()),
    StructField("TAXI_IN", DoubleType()),
    StructField("ARR_TIME", DoubleType()),
    StructField("ARR_DELAY", DoubleType()),
    StructField("CANCELLED", DoubleType()),
    StructField("CANCELLATION_CODE", StringType()),
    StructField("DIVERTED", DoubleType()),
    StructField("AIR_TIME", DoubleType()),
    StructField("DISTANCE", DoubleType()),
    StructField("CARRIER_DELAY", DoubleType()),
    StructField("WEATHER_DELAY", DoubleType()),
    StructField("NAS_DELAY", DoubleType()),
    StructField("SECURITY_DELAY", DoubleType()),
    StructField("LATE_AIRCRAFT_DELAY", DoubleType())
])

# =========================================
# 4️⃣ READ FROM KAFKA & PARSE
# =========================================
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest") # Ensure we grab what was already sent
    .option("maxOffsetsPerTrigger", 5000)   # Smaller batches for stability
    .load()
)

parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("FL_DATE", to_date(col("FL_DATE"), "yyyy-MM-dd"))
    # Safety fill for numeric columns to prevent dashboard math errors
    .fillna(0.0) 
    .withColumn("CANCELLED", col("CANCELLED").cast("int"))
    .withColumn("DIVERTED", col("DIVERTED").cast("int"))
)

# =========================================
# 5️⃣ CREATE TABLE (ICEBERG)
# =========================================
spark.sql("""
CREATE TABLE IF NOT EXISTS local.flight_stream.history_flights (
    FL_DATE DATE,
    OP_UNIQUE_CARRIER STRING,
    TAIL_NUM STRING,
    OP_CARRIER_FL_NUM STRING,
    ORIGIN STRING,
    ORIGIN_CITY_NAME STRING,
    DEST STRING,
    DEST_CITY_NAME STRING,
    DEP_TIME DOUBLE,
    DEP_DELAY DOUBLE,
    TAXI_OUT DOUBLE,
    WHEELS_OFF DOUBLE,
    WHEELS_ON DOUBLE,
    TAXI_IN DOUBLE,
    ARR_TIME DOUBLE,
    ARR_DELAY DOUBLE,
    CANCELLED INT,
    CANCELLATION_CODE STRING,
    DIVERTED INT,
    AIR_TIME DOUBLE,
    DISTANCE DOUBLE,
    CARRIER_DELAY DOUBLE,
    WEATHER_DELAY DOUBLE,
    NAS_DELAY DOUBLE,
    SECURITY_DELAY DOUBLE,
    LATE_AIRCRAFT_DELAY DOUBLE
)
USING ICEBERG
PARTITIONED BY (days(FL_DATE))
""")

# =========================================
# 6️⃣ WRITE STREAM
# =========================================
query = (
    parsed_df
    # Hint to reduce file fragmentation in HDFS
    .repartition(1) 
    .writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(processingTime="30 seconds") # Increased slightly for HDFS stability
    .option("checkpointLocation", CHECKPOINT_PATH)
    .toTable("local.flight_stream.history_flights")
)

print(f"🚀 Ingest started. Checkpoints at: {CHECKPOINT_PATH}")
query.awaitTermination()