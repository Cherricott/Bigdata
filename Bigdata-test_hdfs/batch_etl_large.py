import os
import sys
import gc
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import DoubleType

# =========================================
# 1️⃣ CONFIGURATION
# =========================================
if os.path.exists('/.dockerenv'):
    print("🐳 Running inside Docker")
    warehouse_path = "/app/warehouse"
    base_input_path = "/app/data/"
else:
    print("💻 Running on Host")
    current_dir = os.getcwd()
    warehouse_path = f"file://{current_dir}/warehouse"
    base_input_path = os.path.join(current_dir, "data/")

spark = (
    SparkSession.builder
    .appName("IcebergIngestSmart")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse_path)
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    # Memory Tuning for Laptop
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "4") 
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================================
# 2️⃣ INITIALIZE TABLE (Partitioned)
# =========================================
print("🔨 Initializing Partitioned Table...")
spark.sql("""
CREATE TABLE IF NOT EXISTS local.flight_stream.history_flights (
    FL_DATE DATE,
    OP_UNIQUE_CARRIER STRING,
    ORIGIN STRING,
    DEST STRING,
    DEP_DELAY DOUBLE,
    ARR_DELAY DOUBLE,
    CANCELLED DOUBLE   -- <--- THIS COLUMN IS NEW & REQUIRED
)
USING ICEBERG
PARTITIONED BY (years(FL_DATE))
""")

# =========================================
# 3️⃣ ITERATIVE LOOP
# =========================================
years = range(2018, 2026) 

for yr in years:
    year_path = os.path.join(base_input_path, str(yr))
    
    if not os.path.exists(year_path):
        print(f"⚠️  Skipping {yr} (Folder not found)")
        continue

    print(f"\n🚀 Processing Year: {yr}...")
    
    try:
        # UPDATED: Recursive lookup to be safe
        raw_df = spark.read \
            .option("header", "true") \
            .option("recursiveFileLookup", "true") \
            .csv(year_path)
        
        # Transform
        clean_df = raw_df.select(
            to_date(to_timestamp(col("FL_DATE"), "M/d/y h:m:s a")).alias("FL_DATE"),
            col("OP_UNIQUE_CARRIER"),
            col("ORIGIN"),
            col("DEST"),
            col("DEP_DELAY").cast(DoubleType()),
            col("ARR_DELAY").cast(DoubleType()),
            col("CANCELLED").cast(DoubleType()) # <--- Added this cast
        )
        
        # Write (Append Mode)
        clean_df.writeTo("local.flight_stream.history_flights").append()
        print(f"   ✅ Saved {yr} data to Iceberg.")

        # CLEANUP
        raw_df.unpersist()
        clean_df.unpersist()
        gc.collect()
        
    except Exception as e:
        print(f"   ❌ Error processing {yr}: {e}")

print("\n🎉 All History Ingested.")