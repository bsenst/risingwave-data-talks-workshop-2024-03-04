## Homework in GitHub Codespace

`sudo apt install postgresql-client`

```
psql --version
psql (PostgreSQL) 12.18 (Ubuntu 12.18-0ubuntu0.20.04.1)
```

```
sudo apt-get install -y xdg-utils
```

https://stackoverflow.com/questions/74744493/githubcodespaces-xdg-open-not-working-for-html-files

```
source commands.sh
start-cluster
stream-kafka
```

```
psql
```

# Question 1

```
CREATE MATERIALIZED VIEW trip_time_stats AS
    SELECT
        t1.PULocationID AS zone1,
        t2.DOLocationID AS zone2,
        AVG(t2.tpep_dropoff_datetime - t1.tpep_pickup_datetime) AS avg_trip_time,
        MIN(t2.tpep_dropoff_datetime - t1.tpep_pickup_datetime) AS min_trip_time,
        MAX(t2.tpep_dropoff_datetime - t1.tpep_pickup_datetime) AS max_trip_time
    FROM
        trip_data t1
    JOIN
        trip_data t2 ON t1.PULocationID <> t2.DOLocationID
    WHERE
        t1.tpep_pickup_datetime < t2.tpep_dropoff_datetime
    GROUP BY
        t1.PULocationID, t2.DOLocationID;
```

```
ERROR:  Failed to run the query

Caused by:
  Not supported: streaming nested-loop join
HINT: The non-equal join in the query requires a nested-loop join executor, which could be very expensive to run. Consider rewriting the query to use dynamic filter as a substitute if possible.
See also: https://github.com/risingwavelabs/rfcs/blob/main/rfcs/0033-dynamic-filter.md
```

```
WITH ranked_trip_times AS (
    SELECT
        pickup_zone,
        dropoff_zone,
        avg_trip_time,
        RANK() OVER (ORDER BY avg_trip_time DESC) AS rank
    FROM
        trip_time_stats
)
SELECT
    pickup_zone,
    dropoff_zone,
    avg_trip_time
FROM
    ranked_trip_times
WHERE
    rank = 1;
```

```
...
```

```
CREATE MATERIALIZED VIEW trip_anomalies AS
SELECT
    pickup_zone,
    dropoff_zone,
    avg_trip_time,
    min_trip_time,
    max_trip_time
FROM
    trip_time_stats
WHERE
    avg_trip_time > 1  -- Adjust as needed
    AND (max_trip_time - min_trip_time) > 1; -- Adjust as needed
```

```
...
```

## Question 2

```
CREATE MATERIALIZED VIEW trip_time_stats_with_count AS
SELECT
    t1.PULocationID AS pickup_zone,
    t2.DOLocationID AS dropoff_zone,
    COUNT(*) AS num_trips,
    AVG(t2.trip_time - t1.trip_time) AS avg_trip_time,
    MIN(t2.trip_time - t1.trip_time) AS min_trip_time,
    MAX(t2.trip_time - t1.trip_time) AS max_trip_time
FROM
    trip_data t1
JOIN
    trip_data t2 ON t1.PULocationID <> t2.DOLocationID
                 AND t1.tpep_pickup_datetime < t2.tpep_dropoff_datetime
GROUP BY
    t1.PULocationID, t2.DOLocationID;
```

```
...
```

## Question 3

```
WITH latest_pickup_time AS (
    SELECT MAX(tpep_pickup_datetime) AS latest_time
    FROM trip_data
)

SELECT
    taxi_zone.Zone,
    COUNT(*) AS num_pickups
FROM
    trip_data
JOIN
    taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
CROSS JOIN
    latest_pickup_time
WHERE
    trip_data.tpep_pickup_datetime >= latest_pickup_time.latest_time - INTERVAL '17' HOUR
    AND trip_data.tpep_pickup_datetime <= latest_pickup_time.latest_time
GROUP BY
    taxi_zone.Zone
ORDER BY
    num_pickups DESC
LIMIT 3;
```

```
...
```