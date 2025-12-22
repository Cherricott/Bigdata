from pyspark.sql import SparkSession

# Initialize Spark with the exact same config as your ingestion script
spark = (
    SparkSession.builder
    .appName("Verify_Iceberg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/warehouse")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .getOrCreate()
)

# 2. Run the query
print("------------------------------------------------")
print("📊 Counting rows in Iceberg table...")

try:
    count = spark.sql("SELECT count(*) FROM local.flight_stream.history_flights").collect()[0][0]
    print(f"✅ SUCCESS! Total Rows: {count}")

    # Show top 5 rows just to be sure
    print("------------------------------------------------")
    print("👀 Previewing Data:")
    spark.sql("SELECT * FROM local.flight_stream.history_flights LIMIT 5").show()

except Exception as e:
    print(f"❌ Error: {e}")

print("------------------------------------------------")
