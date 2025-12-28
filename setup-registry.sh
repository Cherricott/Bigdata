#!/bin/sh
set -o errexit

# 1. Create registry container (Port 5001)
reg_name='kind-registry'
reg_port='5001'
if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
  docker run \
    -d --restart=always -p "127.0.0.1:${reg_port}:5000" --name "${reg_name}" \
    registry:2
fi

# 2. Create kind cluster with Host Mounting
echo "Creating cluster with Persistence Bridge..."

# Create the local folders on your computer first
mkdir -p $(pwd)/hadoop_data/namenode

kind create cluster --name bigdata-cluster --config=- <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  # 🛡️ THE PERSISTENCE BRIDGE:
  # This maps your computer's folder to a path inside the Kind Node.
  extraMounts:
  - hostPath: $(pwd)/hadoop_data/namenode
    containerPath: /mnt/hdfs-data
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry]
    config_path = "/etc/containerd/certs.d"
EOF

# 3. Connect registry to the cluster network
if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "${reg_name}")" = 'null' ]; then
  docker network connect "kind" "${reg_name}"
fi

# 4. ConfigMap (Standard Kind requirement)
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${reg_port}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF

# 5. [NEW] THE "MAGIC" PATCH: Force the nodes to see the registry
# This is the part your old script was missing!
echo "Patching Cluster Network for Stability..."
reg_dir="/etc/containerd/certs.d/localhost:${reg_port}"
for node in $(kind get nodes --name bigdata-cluster); do
  docker exec "${node}" mkdir -p "${reg_dir}"
  cat <<EOF | docker exec -i "${node}" tee "${reg_dir}/hosts.toml"
[host."http://${reg_name}:5000"]
EOF
done

echo "✅ SUCCESS! Cluster is reset and fully wired."