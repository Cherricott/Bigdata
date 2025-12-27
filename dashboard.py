import streamlit as st
import pandas as pd
import time
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# === 1. PAGE CONFIG ===
st.set_page_config(
    page_title="Flight Analytics Lakehouse", 
    page_icon="✈️",
    layout="wide"
)

# === 2. SPARK SETUP ===
@st.cache_resource
def get_spark():
    HDFS_NN = "hdfs://namenode:9000"
    HDFS_WAREHOUSE = f"{HDFS_NN}/warehouse"
    return (SparkSession.builder
            .appName("EnhancedDashboard")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", HDFS_WAREHOUSE)
            .config("spark.hadoop.fs.defaultFS", HDFS_NN)
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
            .getOrCreate())

spark = get_spark()

# === 3. VISUALIZATION HELPERS ===
def box_chart(df, group_col, value_col, title):
    if df.empty or group_col not in df.columns or value_col not in df.columns:
        return
    df_plot = df[[group_col, value_col]].dropna()
    if df_plot.empty or df_plot[group_col].nunique() < 2:
        st.info("Collecting more data for distribution...")
        return
    fig, ax = plt.subplots(figsize=(9, 4))
    df_plot.boxplot(column=value_col, by=group_col, ax=ax)
    plt.suptitle("")
    ax.set_title(title)
    plt.xticks(rotation=45)
    st.pyplot(fig)
    plt.close(fig)

# === 4. DATA LOADING FUNCTIONS ===
@st.cache_data(ttl=300)
def load_historical_data():
    try:
        # Load Gold Tables with safety checks
        airline_raw = spark.read.format("iceberg").load("local.flight_stream.airline_stats").toPandas()
        airline_stats = (airline_raw.groupby("OP_UNIQUE_CARRIER")
                         .agg({"avg_dep_delay": "mean", "cancel_pct": "mean", "avg_distance": "mean",
                               "avg_carrier_delay": "mean", "avg_weather_delay": "mean", 
                               "avg_nas_delay": "mean", "avg_late_aircraft_delay": "mean"})
                         .sort_values("avg_dep_delay", ascending=False))
        
        efficiency_raw = spark.read.format("iceberg").load("local.flight_stream.airport_efficiency").toPandas()
        efficiency_stats = (efficiency_raw.groupby("ORIGIN")
                            .agg({"avg_taxi_out": "mean", "avg_taxi_in": "mean"})
                            .sort_values("avg_taxi_out", ascending=False))

        route_stats = spark.read.format("iceberg").load("local.flight_stream.route_stats").toPandas()
        cancel_reasons = spark.read.format("iceberg").load("local.flight_stream.cancel_reasons").toPandas()
        
        trends = spark.read.format("iceberg").load("local.flight_stream.monthly_trends").toPandas().sort_values(["Year", "Month"])
        trends["Period"] = trends["Year"].astype(str) + "-" + trends["Month"].astype(str).str.zfill(2)

        return airline_stats, efficiency_stats, cancel_reasons, trends, route_stats
    except Exception as e:
        st.sidebar.warning(f"Note: Some historical tables are still being generated...")
        return None, None, None, None, None

def load_live_data():
    try:
        spark.catalog.refreshTable("local.flight_stream.history_flights")
        df = spark.read.format("iceberg").load("local.flight_stream.history_flights")
        
        # dynamic find latest date
        latest_date_row = df.selectExpr("max(FL_DATE)").collect()
        latest_date = latest_date_row[0][0] if latest_date_row and latest_date_row[0][0] else None
        
        if latest_date is None: 
            return 0, pd.DataFrame(), None
            
        day_df = df.filter(col("FL_DATE") == latest_date)
        # Convert to Pandas safely
        pdf = day_df.limit(1000).toPandas()
        return day_df.count(), pdf, latest_date
    except: 
        return 0, pd.DataFrame(), None

# === 5. DASHBOARD UI ===
st.title("✈️ Flight Analytics Dashboard")
st.caption("Structured Streaming • Kafka • Apache Iceberg")

tab1, tab2 = st.tabs(["🔴 Real-Time Monitoring", "📊 Historical Insights"])

with tab1:
    total_count, pdf_stream, data_date = load_live_data()
    
    if not pdf_stream.empty:
        # --- Row 1: KPI Metrics (Added Unique Carriers) ---
        m1, m2, m3, m4 = st.columns(4)
        m1.metric(f"Active Date", str(data_date))
        m1.caption(f"Total Processed: {total_count:,}")
        
        # New Metric: Variety of airlines currently flying
        unique_carriers = pdf_stream["OP_UNIQUE_CARRIER"].nunique()
        m2.metric("Active Carriers", unique_carriers)
        
        m3.metric("Diversions", int(pdf_stream["DIVERTED"].sum()) if "DIVERTED" in pdf_stream else 0)
        
        avg_delay = pdf_stream["DEP_DELAY"].mean() if "DEP_DELAY" in pdf_stream else 0
        m4.metric("Avg System Delay", f"{avg_delay:.1f}m")
        
        st.divider()

        # --- Row 2: Charts (Added Hub Activity) ---
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("🏙️ Top Hub Activity (Current Day)")
            # This bar chart will grow and shift in real-time as data streams in
            hub_counts = pdf_stream["ORIGIN"].value_counts().head(10)
            st.bar_chart(hub_counts)
            
        with c2:
            st.subheader("🛫 Real-Time Delay Spread")
            # Keep the box chart, but warn if no delays exist
            if pdf_stream["DEP_DELAY"].max() <= 0:
                st.success("✨ All flights currently on time!")
            box_chart(pdf_stream, "OP_UNIQUE_CARRIER", "DEP_DELAY", "Delay Spread")

        st.divider()

        # --- Row 3: THE LIVE LOG (The most important 'alive' feature) ---
        st.subheader("📋 Recent Records (HDFS Ingest Feed)")
        # Show the most recent flights based on the order they arrived in the dataframe
        display_df = pdf_stream[["OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST", "DEP_TIME", "DEP_DELAY"]].tail(15)
        # Reverse it so newest is at the top
        display_df = display_df.iloc[::-1]
        
        # Style the dataframe to make it look 'live'
        st.dataframe(
            display_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "DEP_DELAY": st.column_config.NumberColumn("Delay (m)", format="%d min"),
                "OP_CARRIER_FL_NUM": "Flight #"
            }
        )

    else:
        st.info("📡 Waiting for live data stream from Kafka... (Check your Producer)")

with tab2:
    h_air, h_eff, h_reasons, h_trends, h_route = load_historical_data()
    if h_air is not None:
        st.subheader("🕵️ Historical Root Causes (Carrier Average)")
        st.bar_chart(h_air[["avg_carrier_delay", "avg_weather_delay", "avg_nas_delay", "avg_late_aircraft_delay"]])

        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1:
            st.subheader("🛫 Airport Congestion")
            st.bar_chart(h_eff["avg_taxi_out"].head(10))
        with c2:
            st.subheader("📏 Avg Flight Distance")
            st.bar_chart(h_air["avg_distance"])
        with c3:
            st.subheader("❌ Cancellation Reasons")
            reason_map = {"A": "Carrier", "B": "Weather", "C": "NAS", "D": "Security"}
            h_reasons["Reason"] = h_reasons["CANCELLATION_CODE"].map(reason_map).fillna("Unknown")
            st.bar_chart(h_reasons.groupby("Reason")["count"].sum())

        st.divider()
        t1, t2 = st.columns([2, 1])
        with t1:
            st.subheader("📈 Industry Delay Trends")
            st.line_chart(h_trends.set_index("Period")["avg_dep_delay"])
        with t2:
            st.subheader("🚩 Most Unreliable Routes")
            # --- FIX: Handling MultiIndex for Route Stats ---
            route_summary = h_route.groupby(["ORIGIN", "DEST"])["total_cancelled"].sum().reset_index()
            route_summary["Route"] = route_summary["ORIGIN"] + " ➡️ " + route_summary["DEST"]
            top_routes = route_summary.sort_values("total_cancelled", ascending=False).head(10)
            st.bar_chart(top_routes.set_index("Route")["total_cancelled"])
    else:
        st.warning("Historical data not found. Please run the Batch Analytics job.")

# === 6. AUTO-REFRESH ===
time.sleep(10)
st.rerun()