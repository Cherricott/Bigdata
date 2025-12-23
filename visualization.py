import time
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# =============================
# CONFIG
# =============================
WAREHOUSE = "/app/warehouse"
REFRESH_INTERVAL = 5  # seconds
REALTIME_LIMIT = 500

st.set_page_config(
    page_title="Flight Streaming Dashboard",
    layout="wide"
)

# =============================
# SPARK
# =============================
@st.cache_resource
def create_spark():
    return (
        SparkSession.builder
        .appName("Visualization")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
        .getOrCreate()
    )

spark = create_spark()
spark.sparkContext.setLogLevel("WARN")

# =============================
# DATA LOADERS
# =============================
@st.cache_data(ttl=300)
def load_airline_stats():
    return (
        spark.read.format("iceberg")
        .load("local.flight_stream.airline_stats")
        .toPandas()
    )

@st.cache_data(ttl=300)
def load_route_stats():
    df = (
        spark.read.format("iceberg")
        .load("local.flight_stream.route_stats")
        .toPandas()
    )
    df["ROUTE"] = df["ORIGIN"] + " → " + df["DEST"]
    return df

def load_realtime(limit=REALTIME_LIMIT):
    return (
        spark.read.format("iceberg")
        .load("local.flight_stream.realtime_flights")
        .orderBy("FL_DATE", ascending=False)
        .limit(limit)
        .toPandas()
    )

# =============================
# CHART COMPONENTS
# =============================
def bar_chart(df, x, y, title, rotate=True):
    if df.empty:
        st.info("No data available")
        return

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(df[x], df[y])
    ax.set_title(title)

    if rotate:
        ax.tick_params(axis="x", rotation=45)

    st.pyplot(fig)
    plt.close(fig)

def horizontal_bar_chart(df, x, y, title):
    if df.empty:
        st.info("No data available")
        return

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(df[y], df[x])
    ax.set_title(title)
    st.pyplot(fig)
    plt.close(fig)

def box_chart(df, group_col, value_col, title):
    df = df[[group_col, value_col]].dropna()

    if df.empty or df[group_col].nunique() < 2:
        st.info("Not enough data")
        return

    fig, ax = plt.subplots(figsize=(9, 4))
    df.boxplot(column=value_col, by=group_col, ax=ax)
    plt.suptitle("")
    ax.set_title(title)
    plt.xticks(rotation=45)
    st.pyplot(fig)
    plt.close(fig)

# =============================
# HISTORICAL SECTION
# =============================
def render_historical():
    st.header("📊 Historical Analysis")

    airline_df = load_airline_stats()
    route_df = load_route_stats()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Avg Departure Delay by Airline")
        bar_chart(
            airline_df.sort_values("avg_dep_delay", ascending=False),
            "OP_UNIQUE_CARRIER",
            "avg_dep_delay",
            "Avg Departure Delay (min)"
        )

    with col2:
        st.subheader("Cancellation Rate by Airline (%)")
        bar_chart(
            airline_df.sort_values("cancel_pct", ascending=False),
            "OP_UNIQUE_CARRIER",
            "cancel_pct",
            "Cancellation Rate (%)"
        )

    st.subheader("Top 10 Worst Routes")
    horizontal_bar_chart(
        route_df.sort_values("avg_dep_delay", ascending=False).head(10),
        "avg_dep_delay",
        "ROUTE",
        "Worst Routes (Avg Delay)"
    )

# =============================
# REALTIME SECTION
# =============================
def render_realtime():
    st.header("🔴 Realtime Monitoring")

    delay_ph = st.empty()
    cancel_ph = st.empty()
    table_ph = st.empty()

    while True:
        df = load_realtime()

        with delay_ph.container():
            st.subheader("Realtime Delay Distribution")
            box_chart(
                df,
                "OP_UNIQUE_CARRIER",
                "DEP_DELAY",
                "Departure Delay Distribution"
            )

        with cancel_ph.container():
            st.subheader("Realtime Cancellation Rate (%)")

            if not df.empty:
                cancel_rate = (
                    df.groupby("OP_UNIQUE_CARRIER")["CANCELLED"]
                    .mean()
                    .reset_index()
                )
                cancel_rate["CANCELLED"] *= 100

                bar_chart(
                    cancel_rate,
                    "OP_UNIQUE_CARRIER",
                    "CANCELLED",
                    "Realtime Cancellation Rate (%)"
                )
            else:
                st.info("Waiting for realtime data...")

        with table_ph.container():
            st.subheader("Latest Events")
            st.dataframe(df.head(20), use_container_width=True)

        time.sleep(REFRESH_INTERVAL)

# =============================
# MAIN
# =============================
def main():
    st.title("✈️ Flight Streaming Dashboard")
    st.caption("Spark • Kafka • Iceberg • Streamlit")

    tab1, tab2 = st.tabs(["📊 Historical", "🔴 Realtime"])

    with tab1:
        render_historical()

    with tab2:
        render_realtime()

main()
