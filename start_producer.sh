#!/bin/bash
SPARK_POD=$(kubectl get pods -l app=spark-client -o jsonpath='{.items[0].metadata.name}')

echo "🔄 Syncing producer code..."
kubectl cp producer.py $SPARK_POD:/app/producer.py

echo "✈️ Starting Producer inside Pod..."
# Passing the internal Kafka address via Env Var to override your 'localhost' logic
kubectl exec -it $SPARK_POD -- env KAFKA_SERVER=kafka:29092 python3 /app/producer.py