#!/bin/bash
SPARK_POD=$(kubectl get pods -l app=spark-client -o jsonpath='{.items[0].metadata.name}')

echo "🔄 Syncing dashboard code..."
kubectl cp dashboard.py $SPARK_POD:/app/dashboard.py

echo "📈 Launching Streamlit inside the pod..."
# Fix: Quotes and sh -c ensures the log stays INSIDE the pod
kubectl exec $SPARK_POD -- sh -c "nohup streamlit run /app/dashboard.py --server.port 8501 --server.headless true > /app/dashboard.log 2>&1 &"

# Give Streamlit 2 seconds to bind to the port
sleep 2

echo "🔗 Opening Tunnel... Open http://localhost:8501"
echo "🛑 (Note: This terminal will stay open to keep the connection alive. Press Ctrl+C to stop the tunnel.)"

kubectl port-forward $SPARK_POD 8501:8501