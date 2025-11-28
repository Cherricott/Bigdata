from pyspark.sql import SparkSession
from pyspark.sql.types import *

# spark = (
#     SparkSession.builder
#     .appName("IcebergStreamingCreateTable")
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
#     .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
#     .config("spark.sql.catalog.local.type", "hadoop")
#     .config("spark.sql.catalog.local.warehouse", "warehouse")
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

# Define schema
schema = StructType([
    StructField("FL_DATE", StringType()),
    StructField("OP_UNIQUE_CARRIER", StringType()),
    StructField("ORIGIN", StringType()),
    StructField("DEST", StringType()),
    StructField("DEP_DELAY", DoubleType()),
    StructField("ARR_DELAY", DoubleType())
])

# Create empty DataFrame with schema
empty_df = spark.createDataFrame([], schema)

# Create Iceberg table if not exists
empty_df.writeTo("local.flight_stream.realtime_flights").createOrReplace()
