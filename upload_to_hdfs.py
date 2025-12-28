import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month

# =========================================================
# 0. CREATE SPARK SESSION (ICEBERG + HDFS)
#    Optimized for a strict 4Gi memory pod limit
# =========================================================
spark = (
    SparkSession.builder
    .appName("HDFS_Iceberg_Month_By_Month_Upload")

    # --- Enable Iceberg SQL extensions ---
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # --- Define Iceberg catalog (Hadoop-based, no Hive Metastore) ---
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config(
        "spark.sql.catalog.local.warehouse",
        "hdfs://namenode:9000/warehouse"
    )

    # --- HDFS default filesystem ---
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")

    # =====================================================
    # MEMORY-SAFETY SETTINGS (CRITICAL)
    # =====================================================
    # Driver + executor capped to avoid OOM in 4Gi pod
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")

    # Very small shuffle to reduce parallelism
    .config("spark.sql.shuffle.partitions", "2")

    # Disable Iceberg fanout writing to avoid many open writers
    .config("spark.sql.iceberg.fanout-enabled", "false")

    .getOrCreate()
)

# Reduce Spark log noise
spark.sparkContext.setLogLevel("WARN")

# =========================================================
# 1. CREATE NAMESPACE AND ICEBERG TABLE (IF NOT EXISTS)
# =========================================================

# Namespace = logical database in Iceberg
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.flight_stream")

# Iceberg table definition
# Partitioned by DAY for efficient pruning & query performance
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

# Years to process
years = range(2018, 2026)

# =========================================================
# 2. YEAR-LEVEL INGESTION LOOP
# =========================================================
for yr in years:
    local_path = f"/app/data/{yr}"

    # Skip year if folder does not exist
    if not os.path.exists(local_path):
        continue

    try:
        print(f"📂 Preparing Year {yr}...")

        # -------------------------------------------------
        # 2.1 READ ALL CSV FILES FOR THE YEAR
        # -------------------------------------------------
        # NOTE: CSV schema is initially all STRING
        raw_df = (
            spark.read
            .option("header", "true")
            .csv(f"file://{local_path}/*.csv")
        )

        # -------------------------------------------------
        # 2.2 YEAR-WIDE CLEANING & TYPE CASTING (LAZY)
        # -------------------------------------------------
        # Done ONCE per year for efficiency
        df_year = (
            raw_df
            .select(
                # Convert string timestamp to DATE
                to_date(
                    col("FL_DATE"),
                    "M/d/yyyy h:mm:ss a"
                ).alias("FL_DATE"),

                # Explicit casting prevents schema drift
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
                col("LATE_AIRCRAFT_DELAY").cast("double"),
            )

            # Replace nulls for boolean-like flags
            .fillna(0, subset=["CANCELLED", "DIVERTED"])

            # Convert to INT after fill
            .withColumn("CANCELLED", col("CANCELLED").cast("int"))
            .withColumn("DIVERTED", col("DIVERTED").cast("int"))

            # Drop rows with invalid dates
            .filter(col("FL_DATE").isNotNull())
        )

        # =================================================
        # 3. MONTH-BY-MONTH WRITE LOOP (MEMORY SAFE)
        # =================================================
        for m in range(1, 13):
            print(f"  🗓️ Processing {yr}-{m:02d}...")

            # Filter only rows for this month
            df_month = df_year.filter(
                month(col("FL_DATE")) == m
            )

            # Avoid empty Iceberg commits
            if df_month.take(1):

                # -------------------------------------------------
                # Reduce parallelism and improve write stability
                # -------------------------------------------------
                df_month = (
                    df_month
                    .repartition(2, "FL_DATE")
                    .sortWithinPartitions("FL_DATE")
                )

                # -------------------------------------------------
                # Append month chunk into Iceberg table
                # -------------------------------------------------
                df_month.writeTo(TABLE_NAME) \
                    .option(
                        "write.parquet.row-group-size-bytes",
                        "67108864"  # 64 MB
                    ) \
                    .option(
                        "write.parquet.page-size-bytes",
                        "1048576"   # 1 MB
                    ) \
                    .append()

                print(f"  ✅ {yr}-{m:02d} uploaded.")

            # Clear Spark cache after each month
            # Prevents memory accumulation across iterations
            spark.catalog.clearCache()

        print(f"🏁 Year {yr} Complete.")

    except Exception as e:
        print(f"❌ Error processing {yr}: {e}")

# =========================================================
# 4. CLEAN SHUTDOWN
# =========================================================
spark.stop()
