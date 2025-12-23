import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc, col

# =========================================
# 1️⃣ CONFIGURATION (HDFS MODE)
# =========================================
# 👇 KEY CHANGE: We now point to the HDFS NameNode, not a local folder
HDFS_NN = "hdfs://namenode:9000"
HDFS_WAREHOUSE = f"{HDFS_NN}/warehouse"

spark = (
    SparkSession.builder
    .appName("IcebergAnalytics")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", HDFS_WAREHOUSE) # <--- Using HDFS
    .config("spark.hadoop.fs.defaultFS", HDFS_NN)               # <--- Default File System
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n🏆 Running Batch Analytics (Gold Layer)...")

# =========================================
# 2️⃣ LOAD DATA (From HDFS)
# =========================================
# This reads the table you created in the previous step (batch_ingest/etl_batch)
history_df = spark.read.format("iceberg").load("local.flight_stream.history_flights")

# =========================================
# 3️⃣ ANALYSIS 1: AIRLINE PERFORMANCE
# =========================================
print("\n--- 1. Generating Airline Stats ---")
airline_stats = (
    history_df.groupBy("OP_UNIQUE_CARRIER")
    .agg(
        count("*").alias("total_flights"),
        avg("DEP_DELAY").alias("avg_dep_delay"),
        (avg("CANCELLED") * 100).alias("cancel_pct")
    )
    .orderBy(desc("avg_dep_delay"))
)

print("📊 Top 5 Worst Airlines (Historical):")
airline_stats.show(5)

# Save Airline Stats (Gold Table)
airline_stats.writeTo("local.flight_stream.airline_stats").createOrReplace()
print("✅ Saved Gold Table: local.flight_stream.airline_stats")


# =========================================
# 4️⃣ ANALYSIS 2: ROUTE QUALITY REPORT
# =========================================
print("\n--- 2. Generating Route Stats ---")
route_stats = (
    history_df.groupBy("ORIGIN", "DEST")
    .agg(
        count("*").alias("total_flights"),
        avg("DEP_DELAY").alias("avg_dep_delay"),
        avg("ARR_DELAY").alias("avg_arr_delay"),
        (avg("CANCELLED") * 100).alias("cancel_pct")
    )
    # Filter: Only look at routes with at least 50 flights to remove noise
    .filter(col("total_flights") > 50) 
    .orderBy(desc("avg_dep_delay"))
)

print("📊 Top 5 Worst Routes (Historical):")
route_stats.show(5)

# Save Route Stats (Gold Table)
route_stats.writeTo("local.flight_stream.route_stats").createOrReplace()
print("✅ Saved Gold Table: local.flight_stream.route_stats")

print("\n🎉 Batch Analytics Complete.")