#!/bin/bash

# 1. Dynamically identify Pod names
SPARK_POD=$(kubectl get pods -l app=spark-client -o jsonpath='{.items[0].metadata.name}')
# Get only the active Running pod for the NameNode
NAMENODE_POD=$(kubectl get pods -l app=namenode --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')

echo "💥 Stopping all Data processes in $SPARK_POD..."

# 2. Kill all processes (Producer, Dashboard, Ingest Engine)
# We kill these first so no new data is being written during the cleanup
kubectl exec $SPARK_POD -- pkill -9 -f "python3"
kubectl exec $SPARK_POD -- pkill -9 -f "streamlit"
kubectl exec $SPARK_POD -- pkill -9 -f "java"

# 3. DELETE June 2025 data and PHYSICALLY PURGE from HDFS
echo "🗑️ Deleting June 2025 data..."
kubectl exec $SPARK_POD -- spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.local.type=hadoop" \
  --conf "spark.sql.catalog.local.warehouse=hdfs://namenode:9000/warehouse" \
  -e "DELETE FROM local.flight_stream.history_flights WHERE FL_DATE >= '2025-06-01';"

echo "🧹 Purging Iceberg snapshots and orphans..."
kubectl exec $SPARK_POD -- spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.local.type=hadoop" \
  --conf "spark.sql.catalog.local.warehouse=hdfs://namenode:9000/warehouse" \
  -e "CALL local.system.expire_snapshots('flight_stream.history_flights'); CALL local.system.remove_orphan_files('flight_stream.history_flights');"

# 4. DATA VERIFICATION: Show what is left in the table
echo "📊 Current Historical Data Status (Jan-May 2025):"
kubectl exec $SPARK_POD -- spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.local.type=hadoop" \
  --conf "spark.sql.catalog.local.warehouse=hdfs://namenode:9000/warehouse" \
  -e "SELECT COUNT(*) as remaining_records FROM local.flight_stream.history_flights;"

# 5. Reset the Checkpoint
echo "🔄 Resetting streaming checkpoints in HDFS..."
if [ ! -z "$NAMENODE_POD" ]; then
    kubectl exec $NAMENODE_POD -- hdfs dfs -rm -r /checkpoints/flights_stream || echo "Checkpoint already clean."
    
    # ============================================================
    # 🛡️ THE PERSISTENCE FIX: Save HDFS Metadata to Disk
    # This prevents the Gold tables and the Deletions from disappearing 
    # when you run 'docker stop'.
    # ============================================================
    echo "💾 Locking HDFS State (Saving Namespace to physical disk)..."
    kubectl exec $NAMENODE_POD -- hdfs dfsadmin -safemode enter
    kubectl exec $NAMENODE_POD -- hdfs dfsadmin -saveNamespace
    kubectl exec $NAMENODE_POD -- hdfs dfsadmin -safemode leave
else
    echo "⚠️ Warning: Could not find NameNode pod to save state."
fi

# 6. Cleaning up Spark temp files and logs
echo "🧹 Cleaning up Spark temp files and logs..."
kubectl exec $SPARK_POD -- rm -rf /tmp/spark-*
kubectl exec $SPARK_POD -- rm -f /app/*.log

echo "✨ System is now reset, PERSISTED, and ready for a fresh demo run."

# 7. Close local tunnels
pkill -f "kubectl port-forward"