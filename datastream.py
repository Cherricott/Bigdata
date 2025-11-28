from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import *

# Initialize Spark
spark = (
    SparkSession.builder
    .appName("ParquetStreaming")
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

# Stream from Parquet (directory)
stream_df = (
    spark.readStream
    .schema(schema)
    .parquet("flights.parquet")  # can also point to a folder
)

# Optional: convert date
stream_df = stream_df.withColumn("FL_DATE", to_date(col("FL_DATE")))

# Print streaming data to console
query = (
    stream_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
