# Databricks notebook source
# MAGIC %md
# MAGIC # Test 07 · RydePro · Questions
# MAGIC
# MAGIC **Medium · ~75-90 min · 6 tasks**
# MAGIC
# MAGIC *Bengaluru · Ride-sharing · Series D · 4,500 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC RydePro is a six-year-old ride-sharing platform operating in 22 Indian cities. Two hundred thousand active drivers. Ten million weekly trips. Driver supply and demand fluctuates by neighbourhood, hour, and weather. The supply ops team optimises driver incentives based on driver-availability patterns.
# MAGIC
# MAGIC ### What just happened
# MAGIC Driver incentive payouts ballooned 30% last quarter without a matching jump in completed trips. The CFO flagged it. On investigation, the supply ops team realised they had been paying drivers based on flawed "online time" calculations. The CDR-like driver_events stream has gaps, duplicates, and out-of-order timestamps. The current logic does not handle any of them.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The VP Supply Ops needs a corrected online-time calculation by end of next sprint. The CFO wants a back-test on Q3 incentive payouts. The data platform lead wants the broadcast joins working so the trip lookups stop OOMing the cluster.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the data engineer on supply ops. Fix the online-time logic, demonstrate broadcast joins for the small dimension lookups, and make sure the chained logic is readable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, LongType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# driver_events: each row is a status change for a driver (online / offline)
driver_events_schema = StructType([
    StructField("event_id", LongType(), False),
    StructField("driver_id", IntegerType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("ingest_ts", TimestampType(), True),
])
driver_events_data = [
    (1,  201, datetime(2025, 7, 1, 6,  0),  "online",  datetime(2025, 7, 1, 6,  0)),
    (2,  201, datetime(2025, 7, 1, 9,  30), "offline", datetime(2025, 7, 1, 9,  31)),
    (3,  201, datetime(2025, 7, 1, 11, 0),  "online",  datetime(2025, 7, 1, 11, 1)),
    (4,  201, datetime(2025, 7, 1, 14, 0),  "offline", datetime(2025, 7, 1, 14, 1)),
    (5,  202, datetime(2025, 7, 1, 7,  0),  "online",  datetime(2025, 7, 1, 7,  1)),
    (6,  202, datetime(2025, 7, 1, 12, 0),  "offline", datetime(2025, 7, 1, 12, 1)),
    (6,  202, datetime(2025, 7, 1, 12, 0),  "offline", datetime(2025, 7, 1, 12, 5)),  # dup, later ingest
    (7,  202, datetime(2025, 7, 1, 13, 0),  "online",  datetime(2025, 7, 1, 13, 1)),
    (8,  202, datetime(2025, 7, 1, 18, 0),  "offline", datetime(2025, 7, 1, 18, 1)),
    (9,  203, datetime(2025, 7, 1, 8,  0),  "online",  datetime(2025, 7, 1, 8,  1)),
    (10, 203, datetime(2025, 7, 1, 9,  0),  "offline", datetime(2025, 7, 1, 9,  1)),
    (11, 203, datetime(2025, 7, 1, 9,  15), "online",  datetime(2025, 7, 1, 9,  16)),
    (12, 203, datetime(2025, 7, 1, 17, 0),  "offline", datetime(2025, 7, 1, 17, 1)),
    (13, 204, datetime(2025, 7, 1, 10, 0),  "online",  datetime(2025, 7, 1, 10, 1)),
    (14, 204, datetime(2025, 7, 1, 20, 0),  "offline", datetime(2025, 7, 1, 20, 1)),
]
driver_events = spark.createDataFrame(driver_events_data, driver_events_schema)
driver_events.createOrReplaceTempView("driver_events")

# driver_metadata: small dimension table (perfect for broadcast)
driver_metadata_schema = StructType([
    StructField("driver_id", IntegerType(), False),
    StructField("driver_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("rating", DoubleType(), True),
])
driver_metadata_data = [
    (201, "Manish Pillai",  "Bengaluru", "sedan",     4.8),
    (202, "Suresh Iyer",    "Bengaluru", "hatchback", 4.6),
    (203, "Rakesh Bhat",    "Bengaluru", "sedan",     4.9),
    (204, "Vijay Hegde",    "Bengaluru", "auto",      4.4),
]
driver_metadata = spark.createDataFrame(driver_metadata_data, driver_metadata_schema)
driver_metadata.createOrReplaceTempView("driver_metadata")

# trips: large fact table (in real life)
trips_schema = StructType([
    StructField("trip_id", LongType(), False),
    StructField("driver_id", IntegerType(), True),
    StructField("trip_start", TimestampType(), True),
    StructField("trip_end", TimestampType(), True),
    StructField("fare", DoubleType(), True),
    StructField("ingest_ts", TimestampType(), True),
])
trips_data = [
    (5001, 201, datetime(2025, 7, 1, 7,  15), datetime(2025, 7, 1, 7,  35), 180.0, datetime(2025, 7, 1, 7,  36)),
    (5002, 201, datetime(2025, 7, 1, 8,  0),  datetime(2025, 7, 1, 8,  25), 220.0, datetime(2025, 7, 1, 8,  26)),
    (5003, 202, datetime(2025, 7, 1, 8,  30), datetime(2025, 7, 1, 9,  0),  300.0, datetime(2025, 7, 1, 9,  1)),
    (5003, 202, datetime(2025, 7, 1, 8,  30), datetime(2025, 7, 1, 9,  0),  300.0, datetime(2025, 7, 1, 9,  10)),  # dup
    (5004, 203, datetime(2025, 7, 1, 10, 0),  datetime(2025, 7, 1, 10, 20), 150.0, datetime(2025, 7, 1, 10, 21)),
    (5005, 204, datetime(2025, 7, 1, 11, 0),  datetime(2025, 7, 1, 11, 45), 250.0, datetime(2025, 7, 1, 11, 46)),
    (5006, 201, datetime(2025, 7, 1, 12, 0),  datetime(2025, 7, 1, 12, 20), 180.0, datetime(2025, 7, 1, 12, 21)),
    (5007, 202, datetime(2025, 7, 1, 15, 0),  datetime(2025, 7, 1, 15, 30), 200.0, datetime(2025, 7, 1, 15, 31)),
    (5008, 203, datetime(2025, 7, 1, 14, 0),  datetime(2025, 7, 1, 14, 25), 175.0, datetime(2025, 7, 1, 14, 26)),
]
trips = spark.createDataFrame(trips_data, trips_schema)
trips.createOrReplaceTempView("trips")

print(f"driver_events: {driver_events.count()}, driver_metadata: {driver_metadata.count()}, trips: {trips.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Each event's previous and next status
# MAGIC
# MAGIC For each driver_event, show the previous event status and the next event status, partitioned by driver_id, ordered by event_ts.
# MAGIC
# MAGIC Output columns: `driver_id`, `event_ts`, `status`, `prev_status`, `next_status`. Sort by driver_id, event_ts.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Total online time per driver
# MAGIC
# MAGIC Compute the total online time per driver for July 1.
# MAGIC
# MAGIC The naive approach is: pair each "online" event with the next "offline" event for the same driver, take the difference, sum across pairs.
# MAGIC
# MAGIC A driver toggles between online and offline multiple times in a day. Sum the time spent online across all their online stretches.
# MAGIC
# MAGIC First deduplicate driver_events on event_id (keeping the latest ingest_ts) before running the logic.
# MAGIC
# MAGIC Output columns: `driver_id`, `total_online_minutes`. Sort by driver_id.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Enrich trips with driver metadata at scale
# MAGIC
# MAGIC Join `trips` with `driver_metadata` to add driver name, city, and vehicle_type. The dimension is small (a few hundred rows in real life); the fact is billions of rows in production and the current join is OOMing the cluster. Write the join — in both SQL and PySpark — so it scales, and in a markdown cell explain when this approach helps and when it would backfire.
# MAGIC
# MAGIC Output columns: `trip_id`, `driver_name`, `city`, `vehicle_type`, `fare`. Sort by trip_id.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC **Your explanation here.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Driver utilisation summary, built in stages
# MAGIC
# MAGIC Build a driver utilisation report using chained CTEs (one CTE per logical step):
# MAGIC
# MAGIC 1. CTE 1: Deduplicate trips by trip_id (keep latest ingest_ts).
# MAGIC 2. CTE 2: Compute per-driver total trip count and total fare.
# MAGIC 3. CTE 3: Join CTE 2 with driver_metadata.
# MAGIC 4. Final SELECT: include rating-band ("premium" if rating >= 4.7, "standard" if >= 4.5, "review" otherwise).
# MAGIC
# MAGIC Output columns: `driver_id`, `driver_name`, `vehicle_type`, `total_trips`, `total_fare`, `rating_band`. Sort by total_fare descending.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Reusing a DataFrame across reports
# MAGIC
# MAGIC The chained CTE result from Task 4 will be reused for downstream reports. Cache the deduplicated trips DataFrame.
# MAGIC
# MAGIC Write code that:
# MAGIC - Caches `trips_dedup` using `.cache()` and triggers materialisation with `.count()`.
# MAGIC - Demonstrates `.persist()` with a non-default storage level (`StorageLevel.MEMORY_AND_DISK`).
# MAGIC - Releases the cache with `.unpersist()`.
# MAGIC - Prints `is_cached` before and after.
# MAGIC
# MAGIC In a markdown cell, briefly explain the difference between cache and persist, and when unpersist matters.

# COMMAND ----------

# Your code here

# COMMAND ----------

# MAGIC %md
# MAGIC **cache vs persist vs unpersist explanation here.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · Combine regional trips with mismatched schemas
# MAGIC
# MAGIC ### Part A
# MAGIC The Mumbai region team just sent an additional trips DataFrame with columns in a DIFFERENT order than the existing trips DataFrame:
# MAGIC
# MAGIC ```
# MAGIC Existing: trip_id, driver_id, trip_start, trip_end, fare, ingest_ts
# MAGIC Mumbai:   driver_id, trip_id, fare, trip_start, trip_end, ingest_ts
# MAGIC ```
# MAGIC
# MAGIC Show that `union` (positional) gives the wrong result, then fix with `unionByName`.
# MAGIC
# MAGIC ### Part B
# MAGIC After the union, deduplicate trips on the COMPOSITE key `(trip_id, driver_id)`, keeping the row with the latest `ingest_ts`.
# MAGIC
# MAGIC Sample Mumbai data is provided below.

# COMMAND ----------

mumbai_trips_data = [
    (301, 6001, 280.0, datetime(2025, 7, 1, 9,  0),  datetime(2025, 7, 1, 9,  30), datetime(2025, 7, 1, 9, 31)),
    (302, 6002, 350.0, datetime(2025, 7, 1, 10, 0),  datetime(2025, 7, 1, 10, 45), datetime(2025, 7, 1, 10,46)),
]
mumbai_trips_schema = StructType([
    StructField("driver_id", IntegerType(), True),
    StructField("trip_id", LongType(), True),
    StructField("fare", DoubleType(), True),
    StructField("trip_start", TimestampType(), True),
    StructField("trip_end", TimestampType(), True),
    StructField("ingest_ts", TimestampType(), True),
])
mumbai_trips = spark.createDataFrame(mumbai_trips_data, mumbai_trips_schema)
mumbai_trips.show()

# COMMAND ----------

# Your union (wrong) demonstration here

# COMMAND ----------

# Your unionByName fix here

# COMMAND ----------

# Your composite-key dedup here

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## What done looks like
# MAGIC
# MAGIC - Task 1: Previous and next event status correctly attached per driver.
# MAGIC - Task 2: Online duration via online-offline pairing pattern.
# MAGIC - Task 3: Join writing scales to billions of rows on the fact side, with explanation.
# MAGIC - Task 4: Chained CTE report with rating bands.
# MAGIC - Task 5: cache, persist, unpersist demonstrated.
# MAGIC - Task 6: union vs unionByName diff shown, composite-key dedup applied.
