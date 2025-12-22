import os
from pyspark.sql import SparkSession

# === CONFIG ===
# (Using the same Docker-safe config we built earlier)
warehouse_path = "/app/warehouse" if os.path.exists('/.dockerenv') else "warehouse"

spark = (
    SparkSession.builder
    .appName("IcebergBatchIngest")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse_path)
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .getOrCreate()
)

print("⏳ Reading Historical Data (Batch Layer)...")

# 1. Read the huge historical file
# Make sure this file is in your 'parquet' folder!
history_df = spark.read.parquet("/app/parquet/merged_flights_fixed.parquet")

# 2. Basic Transformations (Ensure columns match your Streaming schema)
# We select only the columns we care about for the assignment
clean_history = history_df.select(
    "FL_DATE", "OP_UNIQUE_CARRIER", "ORIGIN", "DEST", "DEP_DELAY", "ARR_DELAY"
)

# 3. Create/Overwrite the Historical Table
print("💾 Writing to Iceberg table: local.flight_stream.history_flights")

clean_history.writeTo("local.flight_stream.history_flights") \
    .createOrReplace()

print("✅ Batch Ingestion Complete. Historical data is now queryable.")