# Bigdata
Big data project for class 161703


# Note:

Currenty handle only 7 columns and 6 months of data for ease of testing:

    spark.sql("""
    CREATE TABLE IF NOT EXISTS local.flight_stream.realtime_flights (
        FL_DATE DATE,
        OP_UNIQUE_CARRIER STRING,
        ORIGIN STRING,
        DEST STRING,
        DEP_DELAY DOUBLE,
        ARR_DELAY DOUBLE,
        CANCELLED DOUBLE 
    )
    USING ICEBERG
    """)


# Turn off your firewall and any VPNs

# Initial setup

To run on K8s, first run this commmand:

    ./setup-registry.sh


Then :

    docker build -t localhost:5001/spark-iceberg-app:latest .

    docker push localhost:5001/spark-iceberg-app:latest

And:

    kubectl apply -f 01-storage.yaml

    kubectl apply -f 02-infra.yaml

    kubectl apply -f 03-spark-cluster.yaml 


# Upload Raw Data, Batch Ingestion and Batch analysis

Goal: Move CSVs into the distributed file system. You only need to run this once.

     ./start_batch.sh


Note: currently  works for small ammount of data

Currently batch analysis do:

    Action: It runs complex aggregations that would be too slow to run live on a dashboard.

        Airline Performance: Calculates the average delay and cancellation rate for every airline in the .csv files.

        Route Quality: Calculates the average delay for every Route (Origin → Dest), filtering out rare routes with fewer than 50 flights.


# Real-Time Streaming (Kafka → Iceberg)
    
This requires two terminals.

Terminal 1 (The Producer - Simulated Planes):

    ./start_producer.sh

(Leave this running. It creates the topic and sends data).

Terminal 2 (The Consumer - Spark Streaming):

    ./start_streaming.sh

(Leave this running).

# Dashboard:

Open a third terminal to run:

Terminal 3 (The Dashboard):

    ./start_dashboard.sh

Currently show:

    Tab 1: Real-Time Speed Layer (The "Now")

        Source: Reads local.flight_stream.realtime_flights.

        Behavior: It refreshes every 2 seconds.

        Visuals:

            Total Counter: Ticks up live as your Producer generates fake flights.

            Live Bar Chart: Shows which airline is suffering delays right this second.

            Raw Feed: Shows the last 10 flights that entered the system.

    Tab 2: Historical Batch Layer (The "Past")

        Source: Reads the Gold tables (airline_stats & route_stats) created by the batch script above.

        Behavior: It caches this data (loads it once) because history doesn't change every second.

        Visuals:

            "All-Time" Worst Airlines: Based on 7 years of data.

            "Most Cancelled" Airlines: Who cancels the most flights historically?

            "Worst Routes": A list of the specific flight paths (e.g., JFK to LAX) that are chronically late.

# STOP CUSTER

Make sure to do Ctrl + C in all open terminal then

First run:

    ./stop_all.sh

Then

    docker stop $(docker ps -q --filter name=bigdata-cluster)

# START CLUSTER

First run:

    docker start $(docker ps -a -q --filter name=bigdata-cluster)

Then you can run the producer, streaming and dashboard

# Factory reset:

Make sure to do Ctrl + C in all open terminal then

    ./stop_all.sh

    # 1. Destroy the Kubernetes Cluster (Wipes all Pods, PVCs, and HDFS data)
    kind delete cluster --name bigdata-cluster

    # 2. Kill the local Docker Registry (Wipes the stored images)
    docker rm -f kind-registry

    # 3. Clean the Docker Network (Wipes the bridge between Registry and Kind)
    docker network prune -f

    # 4. Deep clean Docker cache (Optional: Wipes downloaded layers/images)
    docker system prune -a --volumes -f