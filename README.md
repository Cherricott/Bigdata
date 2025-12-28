# Bigdata

Big data project for class 161703

## Note: Turn off your firewall and any VPNs.

# Initial Setup

## Make scripts executable:

    chmod +x *.sh

## CRITICAL: Prepare Local Storage We must create the data folders manually with open permissions before starting. This prevents "Permission Denied" errors if the folders were previously owned by Root.

    # 1. Create the directories
    mkdir -p ./hadoop_data/namenode/namenode-metadata
    mkdir -p ./hadoop_data/namenode/datanode-data

    # 2. Grant open access (Fixes the "Root Lock" issue)
    chmod -R 777 ./hadoop_data

## Setup Registry:

    ./setup-registry.sh

## Build & Push Images:

    docker build -t localhost:5001/spark-iceberg-app:latest .
    docker push localhost:5001/spark-iceberg-app:latest

## Start storage:

    kubectl apply -f 01-storage.yaml

## Start the Infrastructure: We use a specialized script that formats the NameNode and ensures the DataNode connects correctly without crashing.

    ./setup_infra.sh

## Start Spark Cluster:

    kubectl apply -f 03-spark-cluster.yaml 

Wait 60 Seconds, then you can continue.

# Upload Raw Data, Batch Ingestion and Batch Analysis

Goal: Move CSVs into the distributed file system. You only need to run this once. The data will persist on your hard drive even if you stop the cluster.

    ./start_batch.sh

What this does:

    Ingestion: Uploads local CSV data to HDFS and creates Iceberg tables.

    Airline Performance: Calculates the average delay and cancellation rate for every airline.

    Route Quality: Calculates average delay for every Route (Origin → Dest).

# Real-Time Streaming (Kafka → Iceberg)

This requires two terminals.

Terminal 1 (The Producer - Simulated Planes):

    ./start_streaming.sh

(Leave this running).

Terminal 2 (The Consumer - Spark Streaming):

    ./start_producer.sh

(Leave this running. It creates the topic and sends data).
Dashboard

# Open a third terminal to run the Dashboard:

Terminal 3 (The Dashboard):

    ./start_dashboard.sh

Tabs:

    Real-Time Speed Layer (The "Now"):

        Refreshes every 10 seconds.

        Shows live flight counters and delay spikes.

    Historical Batch Layer (The "Past"):

        Reads the "Gold" tables created by the batch script.

        Shows "All-Time" worst airlines and routes based on historical data.

# STOP CLUSTER (Safe Pause)

To stop the cluster without losing your data:

Ctrl + C in all open terminals.

Run the stop script (Saves HDFS state to disk):

    ./stop_all.sh

Stop the containers:

    docker stop $(docker ps -q --filter name=bigdata-cluster)

## To Resume:

    docker start $(docker ps -a -q --filter name=bigdata-cluster)

Wait 60 seconds, then run the dashboard/streaming scripts again.

# FACTORY RESET (Nuclear Option)

Use this to wipe everything clean. After running this, go back to Step 1 (Initial Setup) to start over.


Kill Background Processes:

    ./stop_all.sh

Destroy the Cluster & Registry:

    kind delete cluster --name bigdata-cluster
    docker rm -f kind-registry
    docker network prune -f


Wipe Local Data: We delete the folder completely. The "Initial Setup" section above will handle recreating it correctly.

    sudo rm -rf ./hadoop_data


Clean Docker Cache:

    docker system prune -a --volumes -f