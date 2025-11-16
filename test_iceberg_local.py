from pyspark.sql import SparkSession

# Initialize Spark with Iceberg 1.6.1 for Spark 3.5
spark = (
    SparkSession.builder
    .appName("IcebergLocalTest")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "warehouse")  # local folder for Iceberg tables
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    .getOrCreate()
)

print(f"✅ Spark version: {spark.version}")
print("✅ SparkSession created successfully")

# Create an Iceberg table in the local warehouse directory
spark.sql("""
CREATE TABLE local.db1.sample_tbl (
  id INT,
  name STRING,
  temp DOUBLE
)
USING iceberg
""")

print("✅ Created Iceberg table: local.db1.sample_tbl")

# Insert a few rows
spark.sql("""
INSERT INTO local.db1.sample_tbl VALUES
  (1, 'Hanoi', 26.4),
  (2, 'Da Nang', 27.1),
  (3, 'Ho Chi Minh City', 28.0)
""")

print("✅ Inserted data successfully")

# Query the Iceberg table
df = spark.sql("SELECT * FROM local.db1.sample_tbl")
df.show()

# Verify Iceberg metadata (optional)
spark.sql("SELECT * FROM local.db1.sample_tbl.snapshots").show()

spark.stop()
