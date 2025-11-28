from pyspark.sql import SparkSession
from pyspark.sql.functions import month, concat_ws, when, avg, count, col

spark = (
    SparkSession.builder
    .appName("IcebergBatch")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-warehouse/")  # MinIO path
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.1")
    .getOrCreate()
)

df = spark.read.parquet("C:/Users/ADMIN/Downloads/Bigdata/merged_flights_fixed.parquet")
df = df.dropna(subset=["DEP_DELAY", "ARR_DELAY"])
df = df.withColumn("MONTH", month(col("FL_DATE"))).withColumn("ROUTE", concat_ws("→", col("ORIGIN"), col("DEST")))

# avg_delay (example)
avg_delay = (
    df.groupBy("YEAR", "OP_UNIQUE_CARRIER", "ROUTE")
      .agg(avg("DEP_DELAY").alias("avg_dep_delay"),
           avg("ARR_DELAY").alias("avg_arr_delay"),
           count("*").alias("num_flights"))
)
# write into Iceberg (namespace local.flight_analytics)
avg_delay.writeTo("local.flight_analytics.avg_delay").createOrReplace()

# repeat for other analytics
