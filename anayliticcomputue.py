from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, concat_ws, month, when

# spark = (
#     SparkSession.builder
#     .appName("BatchLayer_Iceberg")
#     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.local.type", "hadoop")
#     .config("spark.sql.catalog.local.warehouse", "/warehouse/iceberg")
#     .getOrCreate()
# )

# Initialize Spark with Iceberg 1.6.1 for Spark 3.5
spark = (
    SparkSession.builder
    .appName("IcebergLocalTest")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "warehouse")  # local folder for Iceberg tables
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .getOrCreate()
)

df = spark.read.parquet("merged_flights_fixed.parquet")


# Clean and enrich
df = df.dropna(subset=["DEP_DELAY", "ARR_DELAY"])
df = df.withColumn("MONTH", month(col("FL_DATE")))
df = df.withColumn("ROUTE", concat_ws("→", col("ORIGIN"), col("DEST")))

# === ANALYTICS 1: Average delay per airline and route ===
avg_delay = (
    df.groupBy("YEAR", "OP_UNIQUE_CARRIER", "ROUTE")
      .agg(
          avg("DEP_DELAY").alias("avg_dep_delay"),
          avg("ARR_DELAY").alias("avg_arr_delay"),
          count("*").alias("num_flights")
      )
)

avg_delay.writeTo("local.flight_analytics.avg_delay").createOrReplace()

# === ANALYTICS 2: Cancellation & Diversion stats ===
cancel_stats = (
    df.groupBy("YEAR", "OP_UNIQUE_CARRIER")
      .agg(
          avg(when(col("CANCELLED") == 1, 1).otherwise(0)).alias("cancel_rate"),
          avg(when(col("DIVERTED") == 1, 1).otherwise(0)).alias("divert_rate")
      )
)
cancel_stats.writeTo("local.flight_analytics.cancel_stats").createOrReplace()

# === ANALYTICS 3: Delay causes ===
delay_causes = (
    df.groupBy("YEAR", "OP_UNIQUE_CARRIER")
      .agg(
          avg("CARRIER_DELAY").alias("avg_carrier_delay"),
          avg("WEATHER_DELAY").alias("avg_weather_delay"),
          avg("NAS_DELAY").alias("avg_nas_delay"),
          avg("SECURITY_DELAY").alias("avg_security_delay"),
          avg("LATE_AIRCRAFT_DELAY").alias("avg_late_aircraft_delay")
      )
)
delay_causes.writeTo("local.flight_analytics.delay_causes").createOrReplace()

print("✅ Batch Layer tables created in Iceberg warehouse.")
