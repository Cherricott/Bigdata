import streamlit as st
import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg

# === PAGE CONFIG ===
st.set_page_config(page_title="Flight Analytics", layout="wide")
st.title("✈️ Real-Time Flight Delay Analytics")

# === SPARK SETUP (Cached to avoid restarting Spark every refresh) ===
@st.cache_resource
def get_spark():
    return (SparkSession.builder
            .appName("Dashboard")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", "/app/warehouse")
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
            .getOrCreate())

spark = get_spark()

# === LAYOUT ===
col1, col2 = st.columns(2)

placeholder1 = col1.empty()
placeholder2 = col2.empty()

# === AUTO REFRESH LOOP ===
while True:
    # 1. FORCE REFRESH: Tell Spark to check for new files on disk
    spark.catalog.refreshTable("local.flight_stream.realtime_flights")

    # 2. Read Real-Time Data
    df_stream = spark.read.format("iceberg").load("local.flight_stream.realtime_flights")
    
    # 2. Convert to Pandas for visualization (Limit to recent data to be fast)
    # We aggregate in Spark first, then bring small data to Pandas
    
    # METRIC 1: Average Delay by Airline
    avg_delay = (df_stream.groupBy("OP_UNIQUE_CARRIER")
                 .agg(avg("DEP_DELAY").alias("Avg Delay"))
                 .orderBy(desc("Avg Delay"))
                 .toPandas())

    # METRIC 2: Total Flights Processed
    total_count = df_stream.count()

    # 3. Render Charts
    with placeholder1.container():
        st.metric(label="Total Flights Processed (Streaming)", value=f"{total_count:,}")
        st.subheader("🔴 Current Delays by Airline")
        st.bar_chart(avg_delay.set_index("OP_UNIQUE_CARRIER"))

    with placeholder2.container():
        st.subheader("Recent Raw Data")
        # Grab last 5 rows
        recent_rows = df_stream.orderBy(desc("FL_DATE")).limit(5).toPandas()
        st.dataframe(recent_rows)

    # Refresh every 2 seconds
    time.sleep(2)
    