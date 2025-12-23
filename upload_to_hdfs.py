# import os
# from pyspark.sql import SparkSession

# # Initialize Spark
# spark = SparkSession.builder \
#     .appName("HDFS_Data_Uploader") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
#     .getOrCreate()

# # Define the years you want to upload (matching your friend's logic)
# years = range(2018, 2026)

# print("🚀 Starting Data Upload to HDFS...")

# for yr in years:
#     # 1. Source: Local Path (inside the container)
#     # We use "file://" to force Spark to look at the container's local disk
#     local_path = f"file:///app/data/{yr}"
    
#     # 2. Destination: HDFS Path
#     hdfs_path = f"hdfs://namenode:9000/data/raw/{yr}"

#     # Check if local folder exists before trying to read
#     if not os.path.exists(f"/app/data/{yr}"):
#         print(f"⚠️  Skipping {yr}: Folder not found in /app/data/")
#         continue

#     try:
#         print(f"   📂 Reading from: {local_path}")
        
#         # Read Local CSVs
#         df = spark.read.option("header", "true").csv(local_path)
        
#         # Write to HDFS
#         # Spark writes data as a folder of part-files, which is perfect for HDFS
#         df.write.mode("overwrite").option("header", "true").csv(hdfs_path)
        
#         print(f"   ✅ {yr} uploaded to HDFS successfully!")
        
#     except Exception as e:
#         print(f"   ❌ Error uploading {yr}: {e}")

# print("🎉 Upload Complete!")
# spark.stop()

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# 1. Initialize Spark with Iceberg and HDFS configs
spark = (SparkSession.builder
    .appName("HDFS_Iceberg_7Col_Upload")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate())

# =========================================================================
# 🆕 CRITICAL BLOCK: INITIALIZE INFRASTRUCTURE
# This ensures the database and table exist after an HDFS wipe.
# =========================================================================
print("🛠️  Initializing Iceberg Catalog and Table...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.flight_stream")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.flight_stream.history_flights (
    FL_DATE DATE,
    OP_UNIQUE_CARRIER STRING,
    ORIGIN STRING,
    DEST STRING,
    DEP_DELAY DOUBLE,
    ARR_DELAY DOUBLE,
    CANCELLED DOUBLE
)
USING ICEBERG
PARTITIONED BY (years(FL_DATE))
""")
# =========================================================================

TABLE_NAME = "local.flight_stream.history_flights"
years = range(2018, 2026)

# The exact 7 columns matching the schema above
TARGET_COLUMNS = [
    "FL_DATE", "OP_UNIQUE_CARRIER", "ORIGIN", "DEST", 
    "DEP_DELAY", "ARR_DELAY", "CANCELLED"
]

print(f"🚀 Starting Column-Aligned Upload to: {TABLE_NAME}")

for yr in years:
    local_path = f"/app/data/{yr}" 
    
    if not os.path.exists(local_path):
        continue

    try:
        print(f"    📂 Processing Year {yr}...")
        
        # Read Local CSV
        raw_df = spark.read.option("header", "true").csv(f"file://{local_path}/*.csv")
        
        # SELECT, CAST, and FORMAT DATE
        df_aligned = raw_df.select(
            to_date(col("FL_DATE"), "M/d/yyyy h:mm:ss a").alias("FL_DATE"),
            col("OP_UNIQUE_CARRIER").cast("string"),
            col("ORIGIN").cast("string"),
            col("DEST").cast("string"),
            col("DEP_DELAY").cast("double"),
            col("ARR_DELAY").cast("double"),
            col("CANCELLED").cast("double")
        )
        
        # Filter out rows with unparseable dates
        df_final = df_aligned.filter(col("FL_DATE").isNotNull())

        # Now .append() will work because the table was created above
        df_final.writeTo(TABLE_NAME).append()
        
        print(f"    ✅ Year {yr} successfully merged into Iceberg table!")
        
    except Exception as e:
        print(f"    ❌ Error processing {yr}: {e}")

print("🎉 DONE! Historical data and Stream data are now unified in Iceberg.")
spark.stop()