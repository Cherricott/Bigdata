import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month

# --- OPTIMIZED SPARK SESSION FOR 4Gi POD LIMITS ---
spark = (SparkSession.builder
    .appName("HDFS_Iceberg_Month_By_Month_Upload")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    
    # --- CRITICAL MEMORY FIXES ---
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    # Low partitions to minimize concurrent Parquet writers
    .config("spark.sql.shuffle.partitions", "2") 
    # Disable Fanout to force sequential writing
    .config("spark.sql.iceberg.fanout-enabled", "false")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# 1. Initialize Schema
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.flight_stream")
spark.sql("""
CREATE TABLE IF NOT EXISTS local.flight_stream.history_flights (
    FL_DATE DATE,
    OP_UNIQUE_CARRIER STRING,
    TAIL_NUM STRING,
    OP_CARRIER_FL_NUM STRING,
    ORIGIN STRING,
    ORIGIN_CITY_NAME STRING,
    DEST STRING,
    DEST_CITY_NAME STRING,
    DEP_TIME DOUBLE,
    DEP_DELAY DOUBLE,
    TAXI_OUT DOUBLE,
    WHEELS_OFF DOUBLE,
    WHEELS_ON DOUBLE,
    TAXI_IN DOUBLE,
    ARR_TIME DOUBLE,
    ARR_DELAY DOUBLE,
    CANCELLED INT,
    CANCELLATION_CODE STRING,
    DIVERTED INT,
    AIR_TIME DOUBLE,
    DISTANCE DOUBLE,
    CARRIER_DELAY DOUBLE,
    WEATHER_DELAY DOUBLE,
    NAS_DELAY DOUBLE,
    SECURITY_DELAY DOUBLE,
    LATE_AIRCRAFT_DELAY DOUBLE
)
USING ICEBERG
PARTITIONED BY (days(FL_DATE))
""")

TABLE_NAME = "local.flight_stream.history_flights"
years = range(2018, 2026)

for yr in years:
    local_path = f"/app/data/{yr}" 
    if not os.path.exists(local_path): continue

    try:
        print(f"📂 Preparing Year {yr}...")
        raw_df = spark.read.option("header", "true").csv(f"file://{local_path}/*.csv")
        
        # Prepare the full year dataframe (Lazy evaluation)
        df_year = raw_df.select(
            to_date(col("FL_DATE"), "M/d/yyyy h:mm:ss a").alias("FL_DATE"),
            col("OP_UNIQUE_CARRIER").cast("string"),
            col("TAIL_NUM").cast("string"),
            col("OP_CARRIER_FL_NUM").cast("string"),
            col("ORIGIN").cast("string"),
            col("ORIGIN_CITY_NAME").cast("string"),
            col("DEST").cast("string"),
            col("DEST_CITY_NAME").cast("string"),
            col("DEP_TIME").cast("double"),
            col("DEP_DELAY").cast("double"),
            col("TAXI_OUT").cast("double"),
            col("WHEELS_OFF").cast("double"),
            col("WHEELS_ON").cast("double"),
            col("TAXI_IN").cast("double"),
            col("ARR_TIME").cast("double"),
            col("ARR_DELAY").cast("double"),
            col("CANCELLED").cast("double"), 
            col("CANCELLATION_CODE").cast("string"),
            col("DIVERTED").cast("double"),
            col("AIR_TIME").cast("double"),
            col("DISTANCE").cast("double"),
            col("CARRIER_DELAY").cast("double"),
            col("WEATHER_DELAY").cast("double"),
            col("NAS_DELAY").cast("double"),
            col("SECURITY_DELAY").cast("double"),
            col("LATE_AIRCRAFT_DELAY").cast("double")
        ).fillna(0, subset=["CANCELLED", "DIVERTED"]) \
         .withColumn("CANCELLED", col("CANCELLED").cast("int")) \
         .withColumn("DIVERTED", col("DIVERTED").cast("int")) \
         .filter(col("FL_DATE").isNotNull())

        # --- SLOW UPLOAD: Process Month by Month ---
        for m in range(1, 13):
            print(f"  🗓️ Processing {yr}-{m:02d}...")
            
            # Filter for just this month
            df_month = df_year.filter(month(col("FL_DATE")) == m)
            
            # Check if there is data for this month before trying to write
            if df_month.take(1):
                # Repartition and Sort for maximum memory protection
                df_month = df_month.repartition(2, "FL_DATE").sortWithinPartitions("FL_DATE")

                # Write the month chunk
                df_month.writeTo(TABLE_NAME) \
                    .option("write.parquet.row-group-size-bytes", "67108864") \
                    .option("write.parquet.page-size-bytes", "1048576") \
                    .append()
                
                print(f"  ✅ {yr}-{m:02d} uploaded.")
            
            # Clear cache/metadata after every month
            spark.catalog.clearCache()
            
        print(f"🏁 Year {yr} Complete.")
        
    except Exception as e:
        print(f"❌ Error processing {yr}: {e}")

spark.stop()