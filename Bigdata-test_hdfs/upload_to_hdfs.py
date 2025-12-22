import os
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("HDFS_Data_Uploader") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Define the years you want to upload (matching your friend's logic)
years = range(2018, 2026)

print("🚀 Starting Data Upload to HDFS...")

for yr in years:
    # 1. Source: Local Path (inside the container)
    # We use "file://" to force Spark to look at the container's local disk
    local_path = f"file:///app/data/{yr}"
    
    # 2. Destination: HDFS Path
    hdfs_path = f"hdfs://namenode:9000/data/raw/{yr}"

    # Check if local folder exists before trying to read
    if not os.path.exists(f"/app/data/{yr}"):
        print(f"⚠️  Skipping {yr}: Folder not found in /app/data/")
        continue

    try:
        print(f"   📂 Reading from: {local_path}")
        
        # Read Local CSVs
        df = spark.read.option("header", "true").csv(local_path)
        
        # Write to HDFS
        # Spark writes data as a folder of part-files, which is perfect for HDFS
        df.write.mode("overwrite").option("header", "true").csv(hdfs_path)
        
        print(f"   ✅ {yr} uploaded to HDFS successfully!")
        
    except Exception as e:
        print(f"   ❌ Error uploading {yr}: {e}")

print("🎉 Upload Complete!")
spark.stop()