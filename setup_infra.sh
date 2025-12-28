#!/bin/bash
# setup_infra.sh

echo "🚀 STARTING HADOOP INFRASTRUCTURE..."

# 1. Start NameNode
echo "📜 Applying configurations..."
kubectl apply -f 02-infra.yaml
kubectl scale deployment datanode --replicas=0

# --- THE FIX IS HERE ---
# Instead of sleep 15, we wait for the Pod to actually be running
echo "⏳ Waiting for NameNode to pull images and start..."
kubectl wait --for=condition=ready pod -l app=namenode --timeout=300s
# -----------------------

# 2. Format NameNode
NN_POD=$(kubectl get pods -l app=namenode -o jsonpath='{.items[0].metadata.name}')
echo "🛠️  Formatting NameNode ($NN_POD)..."
kubectl exec -it $NN_POD -- hdfs namenode -format -force

# 3. Restart NameNode
echo "🔄 Restarting NameNode to apply formatting..."
kubectl delete pod -l app=namenode
kubectl wait --for=condition=ready pod -l app=namenode --timeout=300s

# 4. Enable DataNode
NN_POD=$(kubectl get pods -l app=namenode -o jsonpath='{.items[0].metadata.name}')
echo "🔓 Disabling Safe Mode..."
kubectl exec $NN_POD -- hdfs dfsadmin -safemode leave

echo "▶️  Starting DataNode..."
kubectl scale deployment datanode --replicas=1
kubectl wait --for=condition=ready pod -l app=datanode --timeout=300s

echo "✅ Infrastructure Ready."