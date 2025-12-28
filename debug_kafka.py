from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("KafkaDebug").getOrCreate()

try:
    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "flight_events") \
        .option("startingOffsets", "earliest") \
        .load()

    print("\n" + "="*50)
    print("🔎 KAFKA DATA PREVIEW (TOP 5 ROWS)")
    print("="*50)
    df.selectExpr("CAST(value AS STRING)").show(5, False)

    print("\n" + "="*50)
    print("📊 TOPIC STATS")
    print("="*50)
    df.select("partition", "offset").orderBy("offset", ascending=False).show(5)
    
except Exception as e:
    print(f"❌ ERROR CONNECTING TO KAFKA: {e}")

spark.stop()
