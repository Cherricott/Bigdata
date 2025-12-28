import streamlit as st
import pandas as pd
import time
import matplotlib.pyplot as plt
import altair as alt
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

# --- 3a. ORIGINAL Box Chart Helper (Modified ONLY to Sort) ---
def box_chart(df, group_col, value_col, title):
    if df.empty or group_col not in df.columns or value_col not in df.columns:
        return
    
    # 1. Filter Data
    df_plot = df[[group_col, value_col]].dropna()
    if df_plot.empty or df_plot[group_col].nunique() < 2:
        st.info("Collecting more data for distribution...")
        return
    
    # 2. SORTING LOGIC (New Addition)
    # Calculate median for every carrier and get the order (High -> Low)
    sort_order = df_plot.groupby(group_col)[value_col].median().sort_values(ascending=False).index.tolist()
    
    # Convert column to Categorical type so Matplotlib/Pandas respects the order
    df_plot[group_col] = pd.Categorical(df_plot[group_col], categories=sort_order, ordered=True)
    
    # 3. Original Plotting Logic (Kept exactly as requested)
    fig, ax = plt.subplots(figsize=(9, 4))
    df_plot.boxplot(column=value_col, by=group_col, ax=ax)
    plt.suptitle("")
    ax.set_title(title)
    plt.xticks(rotation=45)
    st.pyplot(fig)
    plt.close(fig)

# --- 3b. ALTAIR Helpers for Tab 2 (Interactive & Sorted) ---
def interactive_bar_chart(df, x_col, y_col, title, sort_order="descending"):
    """Renders a sorted, interactive Altair bar chart with default blue color."""
    if df.empty: return

    if isinstance(df, pd.Series):
        df = df.reset_index()
        df.columns = [x_col, y_col]

    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X(x_col, sort=alt.EncodingSortField(field=y_col, order=sort_order), title=None),
        y=alt.Y(y_col, title=title),
        tooltip=[x_col, y_col]
    ).properties(height=350, title=title).interactive()

    st.altair_chart(chart, use_container_width=True)

def interactive_stacked_chart(df, x_col, stack_cols, title):
    """Renders a stacked bar chart sorted by total value using Altair."""
    if df.empty: return
    
    df_melt = df.reset_index().melt(id_vars=[x_col], value_vars=stack_cols, var_name='Type', value_name='Minutes')
    sort_order = df.sort_values("total_sort", ascending=False).index.tolist()
    
    chart = alt.Chart(df_melt).mark_bar().encode(
        x=alt.X(x_col, sort=sort_order, title=None),
        y=alt.Y('Minutes', stack='zero'),
        color=alt.Color('Type', legend=alt.Legend(title="Delay Type")),
        tooltip=[x_col, 'Type', 'Minutes']
    ).properties(height=400, title=title).interactive()
    
    st.altair_chart(chart, use_container_width=True)

# === 4. DATA LOADING FUNCTIONS ===
@st.cache_data(ttl=300)
def load_historical_data():
    try:
        airline_raw = spark.read.format("iceberg").load("local.flight_stream.airline_stats").toPandas()
        airline_stats = (airline_raw.groupby("OP_UNIQUE_CARRIER")
                         .agg({"avg_dep_delay": "mean", "avg_distance": "mean",
                               "avg_carrier_delay": "mean", "avg_weather_delay": "mean", 
                               "avg_nas_delay": "mean", "avg_late_aircraft_delay": "mean"}))
        
        efficiency_raw = spark.read.format("iceberg").load("local.flight_stream.airport_efficiency").toPandas()
        efficiency_stats = (efficiency_raw.groupby("ORIGIN")
                            .agg({"avg_taxi_out": "mean", "avg_taxi_in": "mean"}))

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
        latest_date_row = df.selectExpr("max(FL_DATE)").collect()
        latest_date = latest_date_row[0][0] if latest_date_row and latest_date_row[0][0] else None
        
        if latest_date is None: 
            return 0, pd.DataFrame(), None
            
        day_df = df.filter(col("FL_DATE") == latest_date)
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
        # --- Row 1: KPI Metrics ---
        m1, m2, m3, m4 = st.columns(4)
        m1.metric(f"Active Date", str(data_date))
        m1.caption(f"Total Processed: {total_count:,}")
        m2.metric("Active Carriers", pdf_stream["OP_UNIQUE_CARRIER"].nunique())
        m3.metric("Diversions", int(pdf_stream["DIVERTED"].sum()) if "DIVERTED" in pdf_stream else 0)
        avg_delay = pdf_stream["DEP_DELAY"].mean() if "DEP_DELAY" in pdf_stream else 0
        m4.metric("Avg System Delay", f"{avg_delay:.1f}m")
        
        st.divider()

        # --- Row 2: Charts ---
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("🏙️ Busiest Hubs (Current Day)")
            hub_counts = pdf_stream["ORIGIN"].value_counts().head(10)
            interactive_bar_chart(hub_counts, "ORIGIN", "count", "Flight Volume")
            
        with c2:
            st.subheader("🛫 Real-Time Delay Spread")
            # --- EXACT REQUESTED LOGIC ---
            # Keep the box chart, but warn if no delays exist
            if pdf_stream["DEP_DELAY"].max() <= 0:
                st.success("✨ All flights currently on time!")
            
            # The box_chart helper now internally handles the sorting
            box_chart(pdf_stream, "OP_UNIQUE_CARRIER", "DEP_DELAY", "Delay Spread")

        st.divider()

        # --- Row 3: THE LIVE LOG ---
        st.subheader("📋 Recent HDFS Ingest Feed")
        log_df = pdf_stream.sort_values(by=["DEP_TIME"], ascending=False, na_position='last').head(20)
        display_cols = ["OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST", "DEP_TIME", "DEP_DELAY"]
        
        def color_delays(val):
            return 'color: red' if val > 15 else 'color: white'

        st.dataframe(
            log_df[display_cols].style.applymap(color_delays, subset=['DEP_DELAY']), 
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
        cols_to_sum = ["avg_carrier_delay", "avg_weather_delay", "avg_nas_delay", "avg_late_aircraft_delay"]
        h_air["total_sort"] = h_air[cols_to_sum].sum(axis=1)
        
        interactive_stacked_chart(h_air, "OP_UNIQUE_CARRIER", cols_to_sum, "Avg Delay Breakout")

        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1:
            st.subheader("🛫 Top 10 Congested Airports")
            eff_plot = h_eff.sort_values("avg_taxi_out", ascending=False).head(10)
            interactive_bar_chart(eff_plot.reset_index(), "ORIGIN", "avg_taxi_out", "Taxi Out (min)")
            
        with c2:
            st.subheader("📏 Avg Flight Distance")
            dist_df = h_air.sort_values("avg_distance", ascending=False).reset_index()
            interactive_bar_chart(dist_df, "OP_UNIQUE_CARRIER", "avg_distance", "Distance (miles)")
            
        with c3:
            st.subheader("❌ Cancellation Reasons")
            reason_map = {"A": "Carrier", "B": "Weather", "C": "NAS", "D": "Security"}
            h_reasons["Reason"] = h_reasons["CANCELLATION_CODE"].map(reason_map).fillna("Unknown")
            reason_counts = h_reasons.groupby("Reason")["count"].sum().reset_index()
            interactive_bar_chart(reason_counts, "Reason", "count", "Total Cancelled")

        st.divider()
        t1, t2 = st.columns([2, 1])
        with t1:
            st.subheader("📈 Industry Delay Trends")
            st.line_chart(h_trends.set_index("Period")["avg_dep_delay"])
        with t2:
            st.subheader("🚩 Most Unreliable Routes")
            route_summary = h_route.groupby(["ORIGIN", "DEST"])["total_cancelled"].sum().reset_index()
            route_summary["Route"] = route_summary["ORIGIN"] + " ➡️ " + route_summary["DEST"]
            top_routes = route_summary.sort_values("total_cancelled", ascending=False).head(10)
            interactive_bar_chart(top_routes, "Route", "total_cancelled", "Total Cancelled")
    else:
        st.warning("Historical data not found. Please run the Batch Analytics job.")

# === 6. AUTO-REFRESH ===
time.sleep(10)
st.rerun()