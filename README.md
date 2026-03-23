# Workshop 7 | Stream Processing

In this homework, we'll practice streaming with Kafka (Redpanda) and PyFlink.

We use Redpanda, a drop-in replacement for Kafka. It implements the same protocol, so any Kafka client library works with it unchanged.

For this homework we will be using Green Taxi Trip data from October 2025:

### Setup
We'll use the same infrastructure from the workshop.

Follow the setup instructions: build the Docker image, start the services:

    cd 07-streaming/workshop/
    docker compose build
    docker compose up -d

This gives us:

- Redpanda (Kafka-compatible broker) on localhost:9092
- Flink Job Manager at http://localhost:8081
- Flink Task Manager
- PostgreSQL on localhost:5432 (user: postgres, password: postgres)

If you previously ran the workshop and have old containers/volumes, do a clean start:

    docker compose down -v
    docker compose build
    docker compose up -d

Note: the container names (like workshop-redpanda-1) assume the directory is called workshop. If you renamed it, adjust accordingly.

<b> Question 1. Redpanda version </b>

Run rpk version inside the Redpanda container:

    sudo docker exec -it de-zoomcamp-hw7-redpanda-1 rpk version

What version of Redpanda are you running?

    Answer: 
        rpk version: v25.3.9

<b> Question 2. Sending data to Redpanda </b>

Create a topic called green-trips:

    sudo docker exec -it de-zoomcamp-hw7-redpanda-1 rpk topic create green-trips

Now write a producer to send the green taxi data to this topic.

Read the parquet file and keep only these columns:

- lpep_pickup_datetime
- lpep_dropoff_datetime
- PULocationID
- DOLocationID
- passenger_count
- trip_distance
- tip_amount
- total_amount

Convert each row to a dictionary and send it to the green-trips topic. You'll need to handle the datetime columns - convert them to strings before serializing to JSON.

Measure the time it takes to send the entire dataset and flush:

    from time import time

    t0 = time()

    # send all rows ...

    producer.flush()

    t1 = time()
    print(f'took {(t1 - t0):.2f} seconds')

How long did it take to send the data?

    Answer: 300 seconds
    

<b> Question 3. Consumer - trip distance </b>

Write a Kafka consumer that reads all messages from the green-trips topic (set auto_offset_reset='earliest').

Count how many trips have a trip_distance greater than 5.0 kilometers.

How many trips have trip_distance > 5?

    Answer: 8506

    select 
        count(*) 
    from processed_events 
    where 
        trip_distance > 5
    ;

<b> Part 2: PyFlink (Questions 4-6) </b>

For the PyFlink questions, you'll adapt the workshop code to work with the green taxi data. The key differences from the workshop:

- Topic name: green-trips (instead of rides)
- Datetime columns use lpep_ prefix (instead of tpep_)
- You'll need to handle timestamps as strings (not epoch milliseconds)

You can convert string timestamps to Flink timestamps in your source DDL:

    lpep_pickup_datetime VARCHAR,
    event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND

Before running the Flink jobs, create the necessary PostgreSQL tables for your results.

Important notes for the Flink jobs:

- Place your job files in workshop/src/job/ - this directory is mounted into the Flink containers at /opt/src/job/
- Submit jobs with: docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/your_job.py
- The green-trips topic has 1 partition, so set parallelism to 1 in your Flink jobs (env.set_parallelism(1)). With higher parallelism, idle consumer subtasks prevent the watermark from advancing.
- Flink streaming jobs run continuously. Let the job run for a minute or two until results appear in PostgreSQL, then query the results. You can cancel the job from the Flink UI at http://localhost:8081
- If you sent data to the topic multiple times, delete and recreate the topic to avoid duplicates: docker exec -it workshop-redpanda-1 rpk topic delete green-trips

<b> Question 4. Tumbling window - pickup location </b>

Create a Flink job that reads from green-trips and uses a 5-minute tumbling window to count trips per PULocationID.

Write the results to a PostgreSQL table with columns: window_start, PULocationID, num_trips.

After the job processes all data, query the results:

    SELECT PULocationID, num_trips
    FROM <your_table>
    ORDER BY num_trips DESC
    LIMIT 3;

Which PULocationID had the most trips in a single 5-minute window?:

    Answer: 74

![query_result4](/pictures/answer4.png)

<b> Question 5. Session window - longest streak </b>

Create another Flink job that uses a session window with a 5-minute gap on PULocationID, using lpep_pickup_datetime as the event time with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other. When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the PULocationID with the longest session (most trips in a single session).

How many trips were in the longest session?

    Answer: 81

    SELECT
        window_start,
        window_end,
        (window_end - window_start) AS Window_duration,
        pulocationid,
        num_trips,
        total_revenue

    FROM processed_events_aggregated_2
    ORDER BY num_trips DESC
    LIMIT 10
    ;

![query_result5](/pictures/answer5.png)

<b> Question 6. Tumbling window - largest tip </b>

Create a Flink job that uses a 1-hour tumbling window to compute the total tip_amount per hour (across all locations).

Which hour had the highest total tip amount?

    Answer: 2025-10-16 18:00:00

     SELECT
        window_start,
        COUNT(pulocationid) AS TotalNoLocations_Window1hr,
        SUM(num_trips) AS TotalNotrips_Window1hr,
        SUM(total_tip_amount) AS TotaTipAmount_Window1hr

    FROM processed_events_aggregated_tip_amount
    GROUP BY
        window_start
    ORDER BY
        TotaTipAmount_Window1hr DESC
    LIMIT 10
    ;

![Query_result6](/pictures/answer6.png)