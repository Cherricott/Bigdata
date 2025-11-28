import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, concat_ws, month, when

# =========================================
# 1️⃣ Cross-Platform Configuration
# =========================================

# --- Windows-Only Hadoop Setup ---
if sys.platform.startswith('win'):
    print("Windows detected: Configuring Hadoop...")
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
else:
    print(f"Running on {sys.platform}: Native Hadoop support enabled.")

# --- Dynamic Warehouse Path ---
current_dir = os.getcwd()
if sys.platform.startswith('win'):
    warehouse_path = "warehouse"
else:
    # Linux needs "file://" to verify it's not HDFS
    warehouse_path = f"file://{current_dir}/warehouse"

# =========================================
# 2️⃣ Initialize Spark
# =========================================
spark = (
    SparkSession.builder
    .appName("IcebergBatchLayer")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse_path)
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================================
# 3️⃣ READ: Ingest Raw Data
# =========================================
# Reads a large historical file (Parquet format)
print("Reading parquet file...")
try:
    df = spark.read.parquet("parquet/merged_flights_fixed.parquet")
except Exception as e:
    print("❌ Error: Could not find 'merged_flights_fixed.parquet'. Make sure the file is in this folder.")
    sys.exit(1)

# =========================================
# 4️⃣ TRANSFORM: Clean & Feature Engineering
# =========================================
# Remove rows where Delay info is missing (useless data)
df = df.dropna(subset=["DEP_DELAY", "ARR_DELAY"])

# Add 'MONTH' column (Extracted from Date)
df = df.withColumn("MONTH", month(col("FL_DATE")))

# Add 'ROUTE' column (e.g., "JFK→LHR")
df = df.withColumn("ROUTE", concat_ws("→", col("ORIGIN"), col("DEST")))

# =========================================
# 5️⃣ AGGREGATE: Create Analytic Tables
# =========================================

# --- TABLE 1: Performance by Route ---
print("Creating Table 1: Avg Delay...")
avg_delay = (
    df.groupBy("YEAR", "OP_UNIQUE_CARRIER", "ROUTE")
      .agg(
          avg("DEP_DELAY").alias("avg_dep_delay"),
          avg("ARR_DELAY").alias("avg_arr_delay"),
          count("*").alias("num_flights")
      )
)
# Save to Iceberg (Create new or overwrite existing)
avg_delay.writeTo("local.flight_analytics.avg_delay").createOrReplace()

# --- TABLE 2: Reliability Stats ---
print("Creating Table 2: Cancel Stats...")
cancel_stats = (
    df.groupBy("YEAR", "OP_UNIQUE_CARRIER")
      .agg(
          # Logic: If Cancelled=1, count it as 1, else 0. The Average of 1s and 0s is the % rate.
          avg(when(col("CANCELLED") == 1, 1).otherwise(0)).alias("cancel_rate"),
          avg(when(col("DIVERTED") == 1, 1).otherwise(0)).alias("divert_rate")
      )
)
cancel_stats.writeTo("local.flight_analytics.cancel_stats").createOrReplace()

# --- TABLE 3: Why were they late? ---
print("Creating Table 3: Delay Causes...")
delay_causes = (
    df.groupBy("YEAR", "OP_UNIQUE_CARRIER")
      .agg(
          avg("CARRIER_DELAY").alias("avg_carrier_delay"),
          avg("WEATHER_DELAY").alias("avg_weather_delay"),
          avg("NAS_DELAY").alias("avg_nas_delay"),
          avg("SECURITY_DELAY").alias("avg_security_delay"),
          avg("LATE_AIRCRAFT_DELAY").alias("avg_late_aircraft_delay")
      )
)
delay_causes.writeTo("local.flight_analytics.delay_causes").createOrReplace()

print("\n✅ Batch Layer tables created successfully in Iceberg warehouse.")