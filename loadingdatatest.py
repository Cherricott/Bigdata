# # =========================================
# # Batch Layer Analytics with Spark + Iceberg
# # =========================================

# # Import necessary libraries
# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import avg, count, col, concat_ws, month, when

# # === Set Hadoop Home (update this to your actual path) ===
# os.environ["HADOOP_HOME"] = "C:\\hadoop"  # folder containing bin\winutils.exe
# os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

# # === Initialize Spark with Iceberg 1.6.1 for Spark 3.5 ===
# spark = (
#     SparkSession.builder
#     .appName("IcebergLocalTest")
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.local.type", "hadoop")
#     .config("spark.sql.catalog.local.warehouse", "warehouse")  # local folder for Iceberg tables
#     .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
#     .getOrCreate()
# )


# # -------------------------------
# # 2️⃣ Load Iceberg Tables
# # -------------------------------
# avg_delay_df = spark.table("local.flight_analytics.avg_delay")
# cancel_stats_df = spark.table("local.flight_analytics.cancel_stats")
# delay_causes_df = spark.table("local.flight_analytics.delay_causes")

# # -------------------------------
# # 3️⃣ Inspect Sample Data
# # -------------------------------
# print("=== Avg Delay Sample ===")
# avg_delay_df.show(5)

# print("=== Cancel Stats Sample ===")
# cancel_stats_df.show(5)

# print("=== Delay Causes Sample ===")
# delay_causes_df.show(5)

# # -------------------------------
# # 4️⃣ Analytics Queries
# # -------------------------------

# # Top 10 routes with highest departure delay
# print("=== Top 10 Routes with Highest Avg Dep Delay ===")
# spark.sql("""
# SELECT ROUTE, OP_UNIQUE_CARRIER, avg_dep_delay
# FROM local.flight_analytics.avg_delay
# ORDER BY avg_dep_delay DESC
# LIMIT 10
# """).show()

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, concat_ws, month, when
# Import AnalysisException to handle missing tables gracefully
from pyspark.sql.utils import AnalysisException

# =========================================
# 1️⃣ Cross-Platform Configuration
# =========================================

# --- A. Windows-Only Hadoop Setup ---
if sys.platform.startswith('win'):
    print("Windows detected: Configuring Hadoop...")
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
else:
    print(f"Running on {sys.platform}: Native Hadoop support enabled.")

# --- B. Dynamic Warehouse Path ---
current_dir = os.getcwd()
if sys.platform.startswith('win'):
    warehouse_path = "warehouse"
else:
    # Linux needs "file://" to avoid HDFS confusion
    warehouse_path = f"file://{current_dir}/warehouse"

print(f"Warehouse location: {warehouse_path}")

# =========================================
# 2️⃣ Initialize Spark
# =========================================
spark = (
    SparkSession.builder
    .appName("IcebergBatchAnalytics")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse_path)
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================================
# 3️⃣ Load & Inspect Iceberg Tables
# =========================================
# We use a helper function to safely load tables, just in case they haven't been created yet.

def safe_load_table(table_name):
    try:
        df = spark.table(table_name)
        print(f"\n✅ Successfully loaded: {table_name}")
        return df
    except Exception as e:
        print(f"\n⚠️  Warning: Table '{table_name}' not found.")
        print("   (Run the batch processing job first to create these tables)")
        return None

# Load the tables
avg_delay_df = safe_load_table("local.flight_analytics.avg_delay")
cancel_stats_df = safe_load_table("local.flight_analytics.cancel_stats")
delay_causes_df = safe_load_table("local.flight_analytics.delay_causes")

# Only proceed if the main table exists
if avg_delay_df:
    # -------------------------------
    # Inspect Sample Data
    # -------------------------------
    print("\n=== Avg Delay Sample ===")
    avg_delay_df.show(5)

    if cancel_stats_df:
        print("=== Cancel Stats Sample ===")
        cancel_stats_df.show(5)

    if delay_causes_df:
        print("=== Delay Causes Sample ===")
        delay_causes_df.show(5)

    # -------------------------------
    # 4️⃣ Analytics Queries
    # -------------------------------
    print("\n=== Top 10 Routes with Highest Avg Dep Delay ===")
    
    # We use a try block here too, just in case the SQL fails due to missing columns
    try:
        spark.sql("""
        SELECT ROUTE, OP_UNIQUE_CARRIER, avg_dep_delay
        FROM local.flight_analytics.avg_delay
        ORDER BY avg_dep_delay DESC
        LIMIT 10
        """).show()
    except Exception as e:
        print(f"Query failed: {e}")

else:
    print("\n❌ Critical: Main tables are missing. Cannot run analytics queries.")

# =========================================
# Cleanup
# =========================================
spark.stop()