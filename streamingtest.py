import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    FloatType, IntegerType
)

# # === Hadoop home for Windows ===
# os.environ["HADOOP_HOME"] = r"C:\hadoop"
# os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

# --- Windows-Only Hadoop Setup ---
if sys.platform.startswith('win'):
    print("Windows detected: Configuring Hadoop...")
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
else:
    print(f"Running on {sys.platform}: Native Hadoop support enabled.")

# === Start Spark session ===
spark = (
    SparkSession.builder
    .appName("ParquetStreamingTest")
    .config("spark.driver.memory", "8g")        # increase driver memory
    .config("spark.executor.memory", "8g")      # increase executor memory
    .config("spark.sql.shuffle.partitions", "2") # reduce number of partitions for small local test
    .getOrCreate()
)

# === Define schema of your Parquet files ===
schema = StructType([
    StructField("FL_DATE", StringType(), True),
    StructField("OP_UNIQUE_CARRIER", StringType(), True),
    StructField("TAIL_NUM", StringType(), True),
    StructField("OP_CARRIER_FL_NUM", LongType(), True),  # bigint
    StructField("ORIGIN", StringType(), True),
    StructField("ORIGIN_CITY_NAME", StringType(), True),
    StructField("DEST", StringType(), True),
    StructField("DEST_CITY_NAME", StringType(), True),
    StructField("DEP_TIME", LongType(), True),
    StructField("DEP_DELAY", FloatType(), True),
    StructField("TAXI_OUT", FloatType(), True),
    StructField("WHEELS_OFF", LongType(), True),
    StructField("WHEELS_ON", LongType(), True),
    StructField("TAXI_IN", FloatType(), True),
    StructField("ARR_TIME", LongType(), True),
    StructField("ARR_DELAY", FloatType(), True),
    StructField("CANCELLED", FloatType(), True),
    StructField("CANCELLATION_CODE", StringType(), True),
    StructField("DIVERTED", FloatType(), True),
    StructField("AIR_TIME", FloatType(), True),
    StructField("DISTANCE", FloatType(), True),
    StructField("CARRIER_DELAY", FloatType(), True),
    StructField("WEATHER_DELAY", FloatType(), True),
    StructField("NAS_DELAY", FloatType(), True),
    StructField("SECURITY_DELAY", FloatType(), True),
    StructField("LATE_AIRCRAFT_DELAY", FloatType(), True)
])

# === Input and checkpoint folders ===
# Get the absolute path of the current folder (Works on Linux & Windows)
current_dir = os.path.abspath(os.getcwd())

# Create full paths
input_path = os.path.join(current_dir, "parquet")
checkpoint_path = os.path.join(current_dir, "checkpoint_parquet")

print(f"Reading data from: {input_path}")
print(f"Saving state to:   {checkpoint_path}")

# === Read streaming Parquet files with schema ===
stream = (
    spark.readStream
    .schema(schema)
    .format("parquet")
    .option("maxFilesPerTrigger", 1)
    .load(input_path)
)

# === Write to console ===
query = (
    stream.writeStream
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

print("✅ Streaming from Parquet started. Ctrl+C to stop.")
query.awaitTermination()
