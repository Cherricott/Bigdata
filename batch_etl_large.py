import os
import sys
import gc  # Garbage Collector interface
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, year
from pyspark.sql.types import DoubleType

# =========================================
# 1️⃣ CONFIGURATION
# =========================================
if os.path.exists('/.dockerenv'):
    warehouse_path = "/app/warehouse"
    base_input_path = "/app/data/"
else:
    current_dir = os.getcwd()
    warehouse_path = f"file://{current_dir}/warehouse"
    base_input_path = os.path.join(current_dir, "data/")

spark = (
    SparkSession.builder
    .appName("IcebergBatchSmart")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse_path)
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    # TUNE FOR LOCAL PERFORMANCE
    .config("spark.sql.shuffle.partitions", "4") # Keep low for local laptop
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================================
# 2️⃣ INITIALIZE TABLE (Partitioned)
# =========================================
# We create the table ONCE with specific partitioning.
# 'PARTITIONED BY (years(FL_DATE))' makes queries faster later.
print("🔨 Initializing Partitioned Table...")
spark.sql("""
CREATE TABLE IF NOT EXISTS local.flight_stream.history_flights (
    FL_DATE DATE,
    OP_UNIQUE_CARRIER STRING,
    ORIGIN STRING,
    DEST STRING,
    DEP_DELAY DOUBLE,
    ARR_DELAY DOUBLE
)
USING ICEBERG
PARTITIONED BY (years(FL_DATE))
""")

# =========================================
# 3️⃣ ITERATIVE PROCESSING LOOP
# =========================================
# Assuming your folders are named "2018", "2019", etc. inside /data/
years_to_process = list(range(2018, 2026)) # 2018 to 2025

for yr in years_to_process:
    year_path = os.path.join(base_input_path, str(yr))
    
    # Check if folder exists before trying to read
    if not os.path.exists(year_path):
        print(f"⚠️  Skipping {yr}: Folder not found at {year_path}")
        continue
        
    print(f"\n🚀 Processing Year: {yr}...")
    
    try:
        # Read only THIS year's CSVs
        raw_df = spark.read.option("header", "true").csv(year_path)
        
        # Transform
        clean_df = raw_df.select(
            to_date(to_timestamp(col("FL_DATE"), "M/d/y h:m:s a")).alias("FL_DATE"),
            col("OP_UNIQUE_CARRIER"),
            col("ORIGIN"),
            col("DEST"),
            col("DEP_DELAY").cast(DoubleType()),
            col("ARR_DELAY").cast(DoubleType())
        )
        
        # WRITE MODE: APPEND
        # We append this year's slice to the main table
        print(f"   💾 Appending {yr} data to Iceberg...")
        clean_df.writeTo("local.flight_stream.history_flights").append()
        
        print(f"   ✅ Finished {yr}")

        # CLEANUP MEMORY
        # This is critical for laptops. We force Spark to forget the previous year.
        raw_df.unpersist()
        clean_df.unpersist()
        gc.collect() 
        
    except Exception as e:
        print(f"   ❌ Failed to process {yr}: {e}")

print("\n🎉 All years processed.")