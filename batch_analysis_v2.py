import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, avg, count, sum, max, min, desc, 
    year, month, dayofmonth, lag, date_sub, datediff, when, lit
)

# =========================================
# 1️⃣ CẤU HÌNH (CONFIGURATION)
# =========================================
if os.path.exists('/.dockerenv'):
    warehouse_path = "/app/warehouse"
else:
    current_dir = os.getcwd()
    warehouse_path = f"file://{current_dir}/warehouse"

spark = (
    SparkSession.builder
    .appName("IcebergAnalyticsAdvanced")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse_path)
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n🏆 Running Advanced Batch Analytics...")

# Load dữ liệu sạch từ Iceberg
df = spark.read.format("iceberg").load("local.flight_stream.history_flights")

# Tách thêm cột Năm, Tháng để group cho dễ (nếu chưa muốn group theo ngày cụ thể)
# Ở đây tôi sẽ group theo FL_DATE (Ngày) như bạn yêu cầu
df_clean = df.withColumn("year", year("FL_DATE")).withColumn("month", month("FL_DATE"))

# =========================================
# 2️⃣ GROUP A: PHÂN TÍCH HÃNG BAY THEO THỜI GIAN
# (Bao gồm: Độ trễ, Tỉ lệ hủy, Khoảng cách)
# =========================================
print("\n--- 1. Airline Stats over Time (Delay, Cancel, Distance) ---")

# Kiểm tra xem có cột DISTANCE không (đề phòng bạn chưa ETL lại)
has_distance = "DISTANCE" in df.columns
dist_expr = avg("DISTANCE").alias("avg_distance") if has_distance else lit(None).alias("avg_distance")

airline_time_stats = (
    df_clean.groupBy("OP_UNIQUE_CARRIER", "year", "month") # Group theo Hãng + Thời gian
    .agg(
        count("*").alias("total_flights"),
        avg("DEP_DELAY").alias("avg_dep_delay"),       # Yêu cầu 1: Trễ theo hãng & tgian
        (avg("CANCELLED") * 100).alias("cancel_pct"),  # Yêu cầu 3: Hủy theo hãng & tgian
        dist_expr                                      # Yêu cầu 5: Khoảng cách tb theo hãng & tgian
    )
    .orderBy("year", "month", desc("avg_dep_delay"))
)

print("📊 Sample Airline Time Series:")
airline_time_stats.show(5)
airline_time_stats.writeTo("local.flight_stream.airline_time_stats").createOrReplace()
print("✅ Saved: local.flight_stream.airline_time_stats")


# =========================================
# 3️⃣ GROUP B: PHÂN TÍCH TUYẾN BAY THEO THỜI GIAN
# (Bao gồm: Độ trễ, Tỉ lệ hủy, Số lượng hủy)
# =========================================
print("\n--- 2. Route Stats over Time (Delay, Cancel Rates & Counts) ---")

route_time_stats = (
    df_clean.groupBy("ORIGIN", "DEST", "year", "month") # Group theo Tuyến + Thời gian
    .agg(
        count("*").alias("total_flights"),
        avg("DEP_DELAY").alias("avg_dep_delay"),        # Yêu cầu 2: Trễ theo tuyến & tgian
        (avg("CANCELLED") * 100).alias("cancel_pct"),   # Yêu cầu 4: Tỉ lệ hủy theo tuyến & tgian
        sum("CANCELLED").alias("total_cancelled")       # Yêu cầu 7: Số lượng hủy theo từng tuyến
    )
    .filter(col("total_flights") > 10) # Lọc bỏ các tuyến quá ít bay để đỡ nhiễu
    .orderBy(desc("total_cancelled"))
)

print("📊 Sample Route Time Series:")
route_time_stats.show(5)
route_time_stats.writeTo("local.flight_stream.route_time_stats").createOrReplace()
print("✅ Saved: local.flight_stream.route_time_stats")


# =========================================
# 4️⃣ GROUP C: SỐ NGÀY HOÃN LIÊN TIẾP (NÂNG CAO)
# (Yêu cầu 6: Số ngày bị hoãn liên tiếp lớn nhất của từng chuyến bay)
# Logic: Đây là bài toán "Gaps and Islands". Cần xác định các chuỗi ngày liên tiếp bị delay > 0.
# =========================================
print("\n--- 3. Analyzing Max Consecutive Delay Days (Advanced) ---")

if "OP_CARRIER_FL_NUM" in df.columns:
    # Bước 1: Chỉ lấy các chuyến bị delay
    delayed_flights = df_clean.filter(col("DEP_DELAY") > 0).select(
        "OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM", "FL_DATE"
    )

    # Bước 2: Dùng Window Function để so sánh ngày hiện tại với ngày trước đó
    # Partition by Hãng + Số hiệu chuyến bay (để định danh 1 chuyến bay cụ thể)
    window_spec = Window.partitionBy("OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM").orderBy("FL_DATE")

    # Tạo cột 'prev_date' là ngày bay trước đó của chuyến này
    input_df = delayed_flights.withColumn("prev_date", lag("FL_DATE", 1).over(window_spec))

    # Bước 3: Tính khoảng cách ngày. Nếu diff > 1 nghĩa là đứt quãng (không liên tiếp)
    # Nếu diff = 1 (hoặc null ở dòng đầu) thì là liên tiếp.
    # Ta tạo một nhóm (island) mới mỗi khi diff > 1.
    input_df = input_df.withColumn(
        "is_new_streak", 
        when(datediff(col("FL_DATE"), col("prev_date")) > 1, 1).otherwise(0)
    )

    # Tạo streak_id bằng cách cộng dồn is_new_streak
    input_df = input_df.withColumn("streak_id", sum("is_new_streak").over(window_spec))

    # Bước 4: Đếm số lượng ngày trong mỗi streak_id
    streak_counts = (
        input_df.groupBy("OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM", "streak_id")
        .count()
        .withColumnRenamed("count", "consecutive_days")
    )

    # Bước 5: Tìm max streak cho mỗi chuyến bay
    max_consecutive_delays = (
        streak_counts.groupBy("OP_UNIQUE_CARRIER", "OP_CARRIER_FL_NUM")
        .agg(max("consecutive_days").alias("max_consecutive_days"))
        .orderBy(desc("max_consecutive_days"))
    )

    print("📊 Top Flights with Longest Consecutive Delays:")
    max_consecutive_delays.show(5)
    
    # Lưu kết quả
    max_consecutive_delays.writeTo("local.flight_stream.consecutive_delays").createOrReplace()
    print("✅ Saved: local.flight_stream.consecutive_delays")

else:
    print("⚠️  SKIPPING Consecutive Analysis: Missing column 'OP_CARRIER_FL_NUM'. Please update ETL.")

print("\n🎉 Advanced Analytics Complete.")