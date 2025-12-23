#!/bin/bash
SPARK_POD=$(kubectl get pods -l app=spark-client -o jsonpath='{.items[0].metadata.name}')

echo "💥 Total Wipeout: Stopping all Data processes in $SPARK_POD..."

# 1. Kill all Python processes (Producer & Dashboard)
kubectl exec $SPARK_POD -- pkill -9 -f "python3"

# 2. Kill all Streamlit processes
kubectl exec $SPARK_POD -- pkill -9 -f "streamlit"

# 3. Kill all Spark/Java processes (This is what actually stops the Ingest engine)
kubectl exec $SPARK_POD -- pkill -9 -f "java"

echo "🧹 Removing any stale logs or temp files..."
kubectl exec $SPARK_POD -- rm -rf /tmp/spark-*
kubectl exec $SPARK_POD -- rm -f /app/*.log

echo "✨ Pod is now a blank slate."

# Add this to the end of stop_all.sh
echo "🌉 Closing any active port-forward tunnels on this machine..."
pkill -f "kubectl port-forward"