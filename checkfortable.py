# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# # === Hadoop home for Windows ===
# os.environ["HADOOP_HOME"] = "C:\\hadoop"  # folder containing bin\winutils.exe
# os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

# # === Spark session with Iceberg ===
# spark = (
#     SparkSession.builder
#     .appName("IcebergCheckTables")
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.local.type", "hadoop")
#     .config("spark.sql.catalog.local.warehouse", "warehouse")  # your Iceberg warehouse folder
#     # .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")  # optional if using --packages
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")

# # === Create namespace if not exists ===
# spark.sql("CREATE NAMESPACE IF NOT EXISTS local.flight_stream")

# # === List all tables in the namespace ===
# tables = spark.sql("SHOW TABLES IN local.flight_stream").collect()
# if not tables:
#     print("No tables found in local.flight_stream")
# else:
#     print("Tables in local.flight_stream:")
#     for t in tables:
#         print(f"- {t['tableName']}")

# # === Optional: Read a streaming table if it exists ===
# table_name = "flights_live"  # replace with your table name

# try:
#     df = spark.read.format("iceberg").table(f"local.flight_stream.{table_name}")
#     print(f"\nPreview of table {table_name}:")
#     df.show(5)
# except Exception as e:
#     print(f"Could not read table {table_name}: {e}")

# spark.stop()

import os
import sys
from pyspark.sql import SparkSession

# === 1. Cross-Platform Setup ===
# Only run the Hadoop fix if on Windows (for your friend)
if sys.platform.startswith('win'):
    print("Windows detected: Configuring Hadoop...")
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
else:
    print(f"Running on {sys.platform}: Native Hadoop support enabled.")

# === 2. Dynamic Warehouse Path ===
# Use current directory to avoid hardcoded paths
current_dir = os.getcwd()
if sys.platform.startswith('win'):
    warehouse_path = "warehouse"
else:
    # Linux needs "file://" to verify it's not HDFS
    warehouse_path = f"file://{current_dir}/warehouse"

print(f"Looking for Iceberg warehouse at: {warehouse_path}")

# === 3. Spark Session ===
spark = (
    SparkSession.builder
    .appName("IcebergCheckTables")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse_path)
    # Ensure the package is loaded so you can run this script standalone
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# === 4. List Tables ===
print("\n--- Checking Namespace ---")
# We list tables in 'flight_stream' (the database you created earlier)
tables = spark.sql("SHOW TABLES IN local.flight_stream").collect()

if not tables:
    print("No tables found in local.flight_stream")
else:
    print("Found the following tables:")
    for t in tables:
        # t.tableName usually returns 'flight_stream.table_name'
        print(f" - {t['tableName']}")

# === 5. Read the Specific Table ===
# I changed this to 'realtime_flights' to match your previous creation code
target_table = "realtime_flights" 

print(f"\n--- Previewing table: {target_table} ---")
try:
    # Load the table
    df = spark.read.format("iceberg").load(f"local.flight_stream.{target_table}")
    
    # Check if empty
    count = df.count()
    print(f"Total Rows: {count}")
    
    if count > 0:
        df.show(5)
    else:
        print("Table exists but is currently empty (Schema created, waiting for data).")
        df.printSchema()
        
except Exception as e:
    print(f"Could not read table '{target_table}'.")
    print(f"Error details: {e}")

spark.stop()