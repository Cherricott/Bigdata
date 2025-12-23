#!/bin/bash
SPARK_POD=$(kubectl get pods -l app=spark-client -o jsonpath='{.items[0].metadata.name}')

echo "🔄 Syncing streaming code..."
kubectl cp stream_ingest_hdfs.py $SPARK_POD:/app/stream_ingest_hdfs.py

echo "🌊 Starting Real-Time Ingest (Background)..."
# Use 'sh -c' and quotes so the redirection happens INSIDE the pod
kubectl exec $SPARK_POD -- sh -c "nohup /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /app/stream_ingest_hdfs.py > /app/ingest.log 2>&1 &"

echo "✅ Streaming started."
echo "💡 To view logs, run: kubectl exec $SPARK_POD -- tail -f /app/ingest.log"