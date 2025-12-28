import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum, max, min, desc, 
    year, month, dayofmonth, lag, date_sub, datediff, when, lit
)
from pyspark.sql.window import Window

# =========================================
# 1️⃣ CONFIGURATION (HDFS MODE)
# =========================================
# 👇 KEY CHANGE: We now point to the HDFS NameNode, not a local folder
HDFS_NN = "hdfs://namenode:9000"
HDFS_WAREHOUSE = f"{HDFS_NN}/warehouse"

spark = (
    SparkSession.builder
    .appName("IcebergAnalytics_Final")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", HDFS_WAREHOUSE)
    .config("spark.hadoop.fs.defaultFS", HDFS_NN)
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    # --- MEMORY & STABILITY FIXES ---
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.driver.memory", "3g")  # Fits inside your 4Gi Pod limit
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "512m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n🏆 Running Batch Analytics (Gold Layer)...")

# =========================================
# 2️⃣ LOAD DATA (From HDFS)
# =========================================
# This reads the table you created in the previous step (batch_ingest/etl_batch)
history_df = spark.read.format("iceberg").load("local.flight_stream.history_flights")

df_clean = history_df.withColumn("year", year("FL_DATE")).withColumn("month", month("FL_DATE"))

# =========================================
# 3️⃣ ANALYSIS 1: AIRLINE PERFORMANCE
# =========================================
# print("\n--- 1. Generating Airline Stats ---")
# airline_stats = (
#     history_df.groupBy("OP_UNIQUE_CARRIER")
#     .agg(
#         count("*").alias("total_flights"),
#         avg("DEP_DELAY").alias("avg_dep_delay"),
#         (avg("CANCELLED") * 100).alias("cancel_pct")
#     )
#     .orderBy(desc("avg_dep_delay"))
# )

# print("📊 Top 5 Worst Airlines (Historical):")
# airline_stats.show(5)

# # Save Airline Stats (Gold Table)
# airline_stats.writeTo("local.flight_stream.airline_stats").createOrReplace()
# print("✅ Saved Gold Table: local.flight_stream.airline_stats")

print("\n--- 1. Airline Stats over Time (Delay, Cancel, Distance) ---")

has_distance = "DISTANCE" in history_df.columns
dist_expr = avg("DISTANCE").alias("avg_distance") if has_distance else lit(None).alias("avg_distance")

airline_stats = (
    df_clean.groupBy("OP_UNIQUE_CARRIER", "year", "month")
    .agg(
        count("*").alias("total_flights"),
        avg("DEP_DELAY").alias("avg_dep_delay"),
        (avg("CANCELLED") * 100).alias("cancel_pct"),
        avg("DISTANCE").alias("avg_distance"),
        # Breakdown of causes
        avg("CARRIER_DELAY").alias("avg_carrier_delay"),
        avg("WEATHER_DELAY").alias("avg_weather_delay"),
        avg("NAS_DELAY").alias("avg_nas_delay"),
        avg("LATE_AIRCRAFT_DELAY").alias("avg_late_aircraft_delay")
    )
    .orderBy("year", "month", desc("avg_dep_delay"))
)

print("📊 Sample Airline Time Series:")
airline_stats.show(5)
airline_stats.writeTo("local.flight_stream.airline_stats").createOrReplace()
print("✅ Saved: local.flight_stream.airline_stats")



# =========================================
# 4️⃣ ANALYSIS 2: ROUTE QUALITY REPORT
# =========================================
# print("\n--- 2. Generating Route Stats ---")
# route_stats = (
#     history_df.groupBy("ORIGIN", "DEST")
#     .agg(
#         count("*").alias("total_flights"),
#         avg("DEP_DELAY").alias("avg_dep_delay"),
#         avg("ARR_DELAY").alias("avg_arr_delay"),
#         (avg("CANCELLED") * 100).alias("cancel_pct")
#     )
#     # Filter: Only look at routes with at least 50 flights to remove noise
#     .filter(col("total_flights") > 50) 
#     .orderBy(desc("avg_dep_delay"))
# )

# print("📊 Top 5 Worst Routes (Historical):")
# route_stats.show(5)

# # Save Route Stats (Gold Table)
# route_stats.writeTo("local.flight_stream.route_stats").createOrReplace()
# print("✅ Saved Gold Table: local.flight_stream.route_stats")

print("\n--- 2. Route Stats over Time (Delay, Cancel Rates & Counts) ---")

route_stats = (
    df_clean.groupBy("ORIGIN", "DEST", "year", "month") # Group theo Tuyến + Thời gian
    .agg(
        count("*").alias("total_flights"),
        avg("DEP_DELAY").alias("avg_dep_delay"),        # Yêu cầu 2: Trễ theo tuyến & tgian
        (avg("CANCELLED") * 100).alias("cancel_pct"),   # Yêu cầu 4: Tỉ lệ hủy theo tuyến & tgian
        sum("CANCELLED").alias("total_cancelled")       # Yêu cầu 7: Số lượng hủy theo từng tuyến
    )
    .filter(col("total_flights") > 10) # Lọc bỏ các tuyến quá ít bay để đỡ nhiễu
    .orderBy(desc("total_cancelled"))
)

print("📊 Sample Route Time Series:")
route_stats.show(5)
route_stats.writeTo("local.flight_stream.route_stats").createOrReplace()
print("✅ Saved: local.flight_stream.route_stats")



# =========================================
# 5️⃣ ANALYSIS 3: HISTORICAL TRENDS (Year-over-Year)
# =========================================
print("\n--- 3. Generating Monthly Trend Stats ---")

trend_stats = (
    history_df.withColumn("Year", year("FL_DATE"))
    .withColumn("Month", month("FL_DATE"))
    .groupBy("Year", "Month")
    .agg(
        avg("DEP_DELAY").alias("avg_dep_delay"),
        avg("ARR_DELAY").alias("avg_arr_delay"),
        count("*").alias("flight_volume")
    )
    .orderBy("Year", "Month")
)

print("📊 Sample of Monthly Trends:")
trend_stats.show(5)

# Save Trend Stats (Gold Table)
trend_stats.writeTo("local.flight_stream.monthly_trends").createOrReplace()
print("✅ Saved Gold Table: local.flight_stream.monthly_trends")



# # =========================================
# # 6️⃣ ANALYSIS 4: CHRONIC DELAYS (FIXED LOGIC)
# # =========================================
# print("\n--- 4. Analyzing Max Consecutive Delay Days ---")
# if "OP_CARRIER_FL_NUM" in df_clean.columns:
#     # Slim down data and group by YEAR to prevent OOM
#     delayed_flights = (df_clean
#         .filter(col("DEP_DELAY") > 0)
#         .select("OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM", "FL_DATE", "year")
#         .distinct()
#     )

#     # Window includes 'year' to keep memory usage low
#     window_spec = Window.partitionBy("OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM", "year").orderBy("FL_DATE")

#     streak_break_df = (delayed_flights
#         .withColumn("prev_date", lag("FL_DATE", 1).over(window_spec))
#         .withColumn("is_new_streak", 
#             when(datediff(col("FL_DATE"), col("prev_date")) > 1, 1)
#             .when(col("prev_date").isNull(), 0)
#             .otherwise(0))
#     )

#     running_window = (Window.partitionBy("OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM", "year")
#                       .orderBy("FL_DATE")
#                       .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    
#     streak_id_df = streak_break_df.withColumn("streak_id", sum("is_new_streak").over(running_window))

#     max_consecutive_delays = (
#         streak_id_df.groupBy("OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM", "year", "streak_id")
#         .count()
#         .groupBy("OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM")
#         .agg(max("count").alias("max_consecutive_days"))
#         .orderBy(desc("max_consecutive_days"))
#     )

#     max_consecutive_delays.coalesce(1).writeTo("local.flight_stream.consecutive_delays").createOrReplace()
#     print("✅ Saved: consecutive_delays")



# =========================================
# 7️⃣ ANALYSIS 5 & 6: EFFICIENCY & CANCELLATIONS
# =========================================

print("\n--- 5. Generating Airport Efficiency Stats ---")
# 1. Force drop the metadata from the catalog and HDFS
spark.sql("DROP TABLE IF EXISTS local.flight_stream.airport_efficiency PURGE")

airport_efficiency = (
    df_clean.groupBy("ORIGIN", "year", "month")
    .agg(
        avg("TAXI_OUT").alias("avg_taxi_out"),
        avg("TAXI_IN").alias("avg_taxi_in"),
        count("*").alias("flight_volume")
    )
    .filter(col("flight_volume") > 100)
    .orderBy(desc("avg_taxi_out"))
)
# 2. Use .create() instead of createOrReplace()
airport_efficiency.coalesce(1).writeTo("local.flight_stream.airport_efficiency").create()
print("✅ Saved: airport_efficiency")


print("\n--- 6. Analyzing Cancellation Reasons ---")
# 1. Force drop
spark.sql("DROP TABLE IF EXISTS local.flight_stream.cancel_reasons PURGE")

cancel_reasons = (
    df_clean.filter(col("CANCELLED") == 1)
    .groupBy("OP_UNIQUE_CARRIER", "CANCELLATION_CODE")
    .count()
    .orderBy(desc("count"))
)
# 2. Use .create()
cancel_reasons.coalesce(1).writeTo("local.flight_stream.cancel_reasons").create()
print("✅ Saved: cancel_reasons")



print("\n🎉 Advanced Analytics Complete.")

