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

# Initial setup

**For Linux/Mac Users:**

Run this in your terminal:

    echo "UID=$(id -u)" > .env
    echo "GID=$(id -g)" >> .env

**For Windows:**

Create a file named `.env` and paste this inside:

    UID=1000
    GID=1000

To run on Docker, first run this commmand:

    docker compose build --no-cache

    docker compose up -d

Then check if it is actually running:

    docker ps

# Run the Batch Layer (History)

Goal: Load the historical CSV data into the history_flights Iceberg table. You only need to run this once.

    docker compose exec spark-app python batch_etl.py


Note: currently batch_etl.py works for small ammount of data, for large ammount of data, try batch_etl_large.py

For Batch analysis, run this:

    docker compose exec spark-app python batch_analysis.py

Currently batch analysis do:

    Action: It runs complex aggregations that would be too slow to run live on a dashboard.

        Airline Performance: Calculates the average delay and cancellation rate for every airline in the .csv files.

        Route Quality: Calculates the average delay for every Route (Origin → Dest), filtering out rare routes with fewer than 50 flights.

    Output: It saves two optimized "Gold" tables to Iceberg:

        local.flight_stream.airline_stats

        local.flight_stream.route_stats

# Run the Speed Layer (Real-Time)

Next run this command to simulate "live" data stream (run it in its own terminal and keep the terminal running):

    docker compose exec spark-app python producer.py

In an new terminal, run this command to get the data into warehouse:

    docker compose exec spark-app python sparkstreaming.py

Check Data Flow: Go to http://localhost:8888 -> Topics -> flights_live -> Messages. (You should see data).

# Run the Serving Layer (Dashboard)

Open a third terminal and run:

    docker compose exec spark-app streamlit run dashboard.py

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

# To stop the code:

Go to the terminal for producer.py and press Ctrl + C

Then to the terminal for sparkstreaming.py and press Ctrl + C

Do the same for dashboard.py

Finally open a new terminal and run

    docker compose down

Run this to confirm

    docker ps

If the table is empty (only header), then you have stopped.
