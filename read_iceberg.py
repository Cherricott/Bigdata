import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg

# === CONFIGURATION ===
# Dynamic Check for Docker vs Local
if os.path.exists('/.dockerenv'):
    warehouse_path = "/app/warehouse"
    kafka_server = "kafka:29092"
else:
    warehouse_path = "warehouse" 
    # (Add Windows logic here if needed, but we assume Docker now)

spark = (
    SparkSession.builder
    .appName("IcebergReader")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse_path)
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("🔍 Watching Iceberg Table: local.flight_stream.realtime_flights")
print("Press Ctrl+C to stop refreshing.\n")

try:
    while True:
        # 1. Load the table (Iceberg allows reading snapshot-isolated data)
        df = spark.read.format("iceberg").load("local.flight_stream.realtime_flights")
        
        # 2. Basic Stats
        total_rows = df.count()
        
        # 3. Analytics: Who is the worst airline right now?
        worst_airline = (
            df.groupBy("OP_UNIQUE_CARRIER")
              .agg(avg("DEP_DELAY").alias("avg_delay"))
              .orderBy(desc("avg_delay"))
              .limit(1)
        )
        
        print(f"--- ⏱️  Update ---")
        print(f"Total Flights Processed: {total_rows}")
        
        if total_rows > 0:
            print("Worst Performing Airline (Live):")
            worst_airline.show()
        
        # Refresh every 5 seconds
        time.sleep(5)
        
except KeyboardInterrupt:
    print("Stopped.")