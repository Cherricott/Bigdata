import streamlit as st
import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, current_date

# === PAGE CONFIG ===
st.set_page_config(
    page_title="Flight Analytics Lakehouse", 
    page_icon="✈️",
    layout="wide"
)

st.title("✈️ Lambda Architecture: Flight Analytics")

# =========================================
# 1️⃣ SPARK SETUP (HDFS CONNECTED)
# =========================================
@st.cache_resource
def get_spark():
    # 👇 KEY CONFIGURATION FOR DOCKER + HDFS
    HDFS_NN = "hdfs://namenode:9000"
    HDFS_WAREHOUSE = f"{HDFS_NN}/warehouse"

    return (SparkSession.builder
            .appName("Dashboard")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            # 👇 Pointing to HDFS
            .config("spark.sql.catalog.local.warehouse", HDFS_WAREHOUSE)
            .config("spark.hadoop.fs.defaultFS", HDFS_NN)
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
            .getOrCreate())

spark = get_spark()

# =========================================
# 2️⃣ DATA LOADING FUNCTIONS
# =========================================

# A. Historical Data (Cached Data)
@st.cache_data
def load_historical_data():
    try:
        # Load the Gold Tables created in batch_analysis.py
        airline_stats = spark.read.format("iceberg").load("local.flight_stream.airline_stats").toPandas()
        route_stats = spark.read.format("iceberg").load("local.flight_stream.route_stats").toPandas()
        return airline_stats, route_stats
    except Exception as e:
        st.error(f"Could not load history: {e}")
        return None, None

# B. Live Data (No Cache)
def load_live_data():
    # Force catalog refresh to see new files
    # 👇 Based on your logs, the stream writes to 'history_flights'
    table_name = "local.flight_stream.history_flights" 
    
    spark.catalog.refreshTable(table_name)
    
    # Optimization: Filter for TODAY only so we don't read 7 years of data every second
    df = spark.read.format("iceberg").load(table_name)
    return df.filter(col("FL_DATE") == current_date())

# =========================================
# 3️⃣ VISUALIZATION LAYOUT
# =========================================

tab1, tab2 = st.tabs(["📡 Real-Time Speed Layer", "📚 Historical Batch Layer"])

# --- TAB 1: LIVE STREAM ---
with tab1:
    st.header("Live Flight Stream (Today)")
    
    # Load Data
    try:
        df_stream = load_live_data()
        # Convert to Pandas for Streamlit (Limit to 1000 rows for speed)
        pdf_stream = df_stream.orderBy(desc("FL_DATE")).limit(1000).toPandas()
        
        total_count = len(pdf_stream)
        
        # 1. Metrics Row
        col1, col2, col3 = st.columns(3)
        col1.metric("Flights Today", f"{total_count:,}")
        
        if total_count > 0:
            # 2. Charts
            avg_delay_live = (pdf_stream.groupby("OP_UNIQUE_CARRIER")["DEP_DELAY"].mean()
                              .sort_values(ascending=False))
            
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("🔴 Current Delays (Live)")
                st.bar_chart(avg_delay_live)
                
            with c2:
                st.subheader("📥 Incoming Feed")
                st.dataframe(pdf_stream[["FL_DATE", "OP_UNIQUE_CARRIER", "ORIGIN", "DEST", "DEP_DELAY", "CANCELLED"]].head(10))
        else:
            st.info("Waiting for today's flights... Make sure Producer is running!")
            
    except Exception as e:
        st.warning(f"Waiting for table creation... ({e})")

# --- TAB 2: HISTORY ---
with tab2:
    st.header("Historical Insights (2018-2025)")
    
    # Load Cached Data
    hist_airlines, hist_routes = load_historical_data()
    
    if hist_airlines is not None:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏆 Airline Performance (All Time)")
            hist_airlines = hist_airlines.sort_values(by="avg_dep_delay", ascending=False)
            st.bar_chart(hist_airlines.set_index("OP_UNIQUE_CARRIER")["avg_dep_delay"])
            
        with col2:
            st.subheader("⚠️ Most Cancelled Airlines")
            st.bar_chart(hist_airlines.set_index("OP_UNIQUE_CARRIER")["cancel_pct"])

        st.subheader("🗺️ Worst Routes of All Time")
        st.dataframe(hist_routes.head(10))
        
    else:
        st.error("Historical tables not found. Run 'batch_analysis.py' first!")
        if st.button("Refresh History"):
            st.cache_data.clear()

# =========================================
# 4️⃣ AUTO-REFRESH
# =========================================
time.sleep(10)
st.rerun()