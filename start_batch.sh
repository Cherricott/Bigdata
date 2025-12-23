#!/bin/bash
SPARK_POD=$(kubectl get pods -l app=spark-client -o jsonpath='{.items[0].metadata.name}')

echo "🔄 Syncing files to pod..."
kubectl cp upload_to_hdfs.py $SPARK_POD:/app/upload_to_hdfs.py
kubectl cp batch_analysis.py $SPARK_POD:/app/batch_analysis.py

echo "📤 Running Historical Upload..."
kubectl exec -it $SPARK_POD -- /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  /app/upload_to_hdfs.py

echo "📊 Running Batch Analysis..."
kubectl exec -it $SPARK_POD -- /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  /app/batch_analysis.py