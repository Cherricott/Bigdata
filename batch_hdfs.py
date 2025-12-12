import gc
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import DoubleType

# =========================================
# 1️⃣ CONFIGURATION - HDFS PATHS
# =========================================

HDFS_NN = "hdfs://hdfs-namenode-0.hdfs-namenode:9000"

HDFS_WAREHOUSE = f"{HDFS_NN}/warehouse"
HDFS_INPUT_BASE = f"{HDFS_NN}/data/raw"

print("📂 HDFS Warehouse:", HDFS_WAREHOUSE)
print("📂 HDFS Input Base:", HDFS_INPUT_BASE)

spark = (
    SparkSession.builder
    .appName("HDFS_to_Iceberg_Ingest")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", HDFS_WAREHOUSE)
    .config("spark.hadoop.fs.defaultFS", HDFS_NN)
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =========================================
# 2️⃣ INITIALIZE ICEBERG TABLE ON HDFS
# =========================================

print("🔨 Creating Iceberg Table in HDFS...")

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

# =========================================
# 3️⃣ ITERATE THROUGH RAW YEARS IN HDFS
# =========================================
years = range(2018, 2026)

for yr in years:

    year_path = f"{HDFS_INPUT_BASE}/{yr}"

    print(f"\n🚀 Processing year {yr}: {year_path}")

    try:
        # Read raw data
        raw_df = (
            spark.read
            .option("header", "true")
            .option("recursiveFileLookup", "true")
            .csv(year_path)
        )

        clean_df = raw_df.select(
            to_date(to_timestamp(col("FL_DATE"), "M/d/y h:m:s a")).alias("FL_DATE"),
            col("OP_UNIQUE_CARRIER"),
            col("ORIGIN"),
            col("DEST"),
            col("DEP_DELAY").cast(DoubleType()),
            col("ARR_DELAY").cast(DoubleType()),
            col("CANCELLED").cast(DoubleType()),
        )

        clean_df.writeTo("local.flight_stream.history_flights").append()

        print(f"   ✅ {yr} written to Iceberg (HDFS warehouse).")

        raw_df.unpersist()
        clean_df.unpersist()
        gc.collect()

    except Exception as e:
        print(f"   ❌ Error processing {yr}: {e}")

print("\n🎉 All HDFS → Iceberg Ingestion Complete!")
