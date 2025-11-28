# Bigdata
Big data project for class 161703

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

Next run this command to simulate "live" data stream (run it in its own terminal and keep the terminal running):

    docker compose exec spark-app python producer.py

In an new terminal, run this command to get the data into warehouse:

    docker compose exec spark-app python sparkstreaming.py

Run this in a new terminal to see: Every 5 seconds, the "Total Flights" number will jump up, and the "Worst Airline" might change. This confirms your pipeline is end-to-end.

    docker compose exec spark-app python read_iceberg.py

Check Data Flow: Go to http://localhost:8888 -> Topics -> flights_live -> Messages. (You should see data).

# To stop the code:

Go to the terminal for producer.py and press Ctrl + C
Then to the terminal for sparkstreaming.py and press Ctrl + C
Next the terminal for read_iceberg.py and press Ctrl + C

Finally open a new terminal and run

    docker compose down

Run this to confirm

    docker ps

If the table is empty (only header), then you have stop.
