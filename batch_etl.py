import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import DoubleType

# =========================================
# 1️⃣ CONFIGURATION
# =========================================
if os.path.exists('/.dockerenv'):
    print("🐳 Running inside Docker")
    warehouse_path = "/app/warehouse"
    input_path = "/app/data/"
else:
    print("💻 Running on Host")
    current_dir = os.getcwd()
    warehouse_path = f"file://{current_dir}/warehouse"
    input_path = os.path.join(current_dir, "data/")

# =========================================
# 🔍 DEBUG: VERIFY FILES EXIST
# =========================================
print(f"\n🔍 DEBUG: Checking for CSV files in {input_path}...")
csv_count = 0
for root, dirs, files in os.walk(input_path):
    for file in files:
        if file.endswith(".csv"):
            print(f"   -> Found: {os.path.join(root, file)}")
            csv_count += 1
            if csv_count >= 3: break # Stop printing after 3 to save space

if csv_count == 0:
    print("❌ ERROR: No CSV files found! Check your 'data' folder structure.")
    print("   Make sure you moved 'parquet/raw_csvs' files into 'data/YEAR/'.")
    sys.exit(1)
else:
    print(f"✅ Found CSV files. Proceeding with Spark...\n")

# =========================================
# 2️⃣ INITIALIZE SPARK
# =========================================
spark = (
    SparkSession.builder
    .appName("IcebergBatchETL")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse_path)
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================================
# 3️⃣ LOAD & CLEAN RAW DATA
# =========================================
try:
    # UPDATED: Added 'recursiveFileLookup' to find files inside Year folders
    raw_df = spark.read \
        .option("header", "true") \
        .option("recursiveFileLookup", "true") \
        .csv(input_path)
    
    # TRANSFORMATION LOGIC
    clean_df = raw_df.select(
        # 1. Parse Date Format: "1/1/2020 12:00:00 AM" -> DateType
        to_date(to_timestamp(col("FL_DATE"), "M/d/y h:m:s a")).alias("FL_DATE"),
        
        # 2. Keep IDs
        col("OP_UNIQUE_CARRIER"),
        col("ORIGIN"),
        col("DEST"),
        
        # 3. Cast Delays to Numbers
        col("DEP_DELAY").cast(DoubleType()),
        col("ARR_DELAY").cast(DoubleType())
    )

    print("📊 Schema Validation:")
    clean_df.printSchema()

    # =========================================
    # 4️⃣ WRITE TO ICEBERG (BATCH LAYER)
    # =========================================
    print("💾 Writing to Iceberg Table: local.flight_stream.history_flights...")
    
    clean_df.writeTo("local.flight_stream.history_flights") \
            .createOrReplace()
            
    print(f"✅ Success! Ingested {clean_df.count()} historical rows.")

except Exception as e:
    print(f"❌ Error: {e}")