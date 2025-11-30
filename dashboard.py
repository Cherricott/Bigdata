import streamlit as st
import pandas as pd
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, count

# === PAGE CONFIG ===
st.set_page_config(
    page_title="Flight Analytics Lakehouse", 
    page_icon="✈️",
    layout="wide"
)

st.title("✈️ Lambda Architecture: Flight Analytics")

# =========================================
# 1️⃣ SPARK SETUP (Cached Resource)
# =========================================
# We use cache_resource so we don't start a new Spark Session every refresh
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

# =========================================
# 2️⃣ DATA LOADING FUNCTIONS
# =========================================

# A. Historical Data (Cached Data)
# Only re-runs if you clear cache or restart. Fast!
@st.cache_data
def load_historical_data():
    try:
        # Load the Gold Tables we created in batch_analysis.py
        airline_stats = spark.read.format("iceberg").load("local.flight_stream.airline_stats").toPandas()
        route_stats = spark.read.format("iceberg").load("local.flight_stream.route_stats").toPandas()
        return airline_stats, route_stats
    except Exception as e:
        return None, None

# B. Live Data (No Cache)
# Always fetches the latest snapshot from Iceberg
def load_live_data():
    # Force catalog refresh to see new files
    spark.catalog.refreshTable("local.flight_stream.realtime_flights")
    return spark.read.format("iceberg").load("local.flight_stream.realtime_flights")

# =========================================
# 3️⃣ VISUALIZATION LAYOUT
# =========================================

# Create two tabs
tab1, tab2 = st.tabs(["📡 Real-Time Speed Layer", "📚 Historical Batch Layer"])

# --- TAB 1: LIVE STREAM ---
with tab1:
    st.header("Live Flight Stream")
    
    # Load Data
    df_stream = load_live_data()
    total_count = df_stream.count()
    
    # 1. Metrics Row
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Flights Processed", f"{total_count:,}")
    
    if total_count > 0:
        # 2. Aggregations (Spark side)
        avg_delay_live = (df_stream.groupBy("OP_UNIQUE_CARRIER")
                          .agg(avg("DEP_DELAY").alias("Avg Delay"))
                          .orderBy(desc("Avg Delay"))
                          .toPandas())
        
        # 3. Charts
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("🔴 Current Delays (Live)")
            st.bar_chart(avg_delay_live.set_index("OP_UNIQUE_CARRIER"))
            
        with c2:
            st.subheader("📥 Incoming Feed")
            recent_rows = df_stream.orderBy(desc("FL_DATE")).limit(10).toPandas()
            st.dataframe(recent_rows[["FL_DATE", "OP_UNIQUE_CARRIER", "ORIGIN", "DEST", "DEP_DELAY", "CANCELLED"]])
    else:
        st.warning("Waiting for data... Start the Producer!")

# --- TAB 2: HISTORY ---
with tab2:
    st.header("Historical Insights (2018-2025)")
    
    # Load Cached Data
    hist_airlines, hist_routes = load_historical_data()
    
    if hist_airlines is not None:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏆 Airline Performance (All Time)")
            # Sort for better chart
            hist_airlines = hist_airlines.sort_values(by="avg_dep_delay", ascending=False)
            st.bar_chart(hist_airlines.set_index("OP_UNIQUE_CARRIER")["avg_dep_delay"])
            st.caption("Average Departure Delay (Minutes)")
            
        with col2:
            st.subheader("⚠️ Most Cancelled Airlines")
            st.bar_chart(hist_airlines.set_index("OP_UNIQUE_CARRIER")["cancel_pct"])
            st.caption("Cancellation Rate (%)")

        st.subheader("🗺️ Worst Routes of All Time")
        st.dataframe(hist_routes.head(10))
        
    else:
        st.error("Historical tables not found. Run 'batch_analysis.py' first!")
        if st.button("Refresh History"):
            st.cache_data.clear()

# =========================================
# 4️⃣ AUTO-REFRESH MECHANISM
# =========================================
# This tells Streamlit to re-run the entire script every 2 seconds
time.sleep(2)
st.rerun()