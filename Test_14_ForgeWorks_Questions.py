# Databricks notebook source
# MAGIC %md
# MAGIC # Test 14 · ForgeWorks Industries · Questions
# MAGIC
# MAGIC **Hard · ~120 min · 5 tasks**
# MAGIC
# MAGIC *Pune · Manufacturing (IoT) · Listed · 8,200 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC ForgeWorks operates three large foundries near Pune and Aurangabad. Each has hundreds of CNC machines instrumented with sensors logging vibration, temperature, and spindle RPM every 10 seconds. The OT data flows through OPC UA into the Azure cloud, lands in Delta, and feeds OEE (Overall Equipment Effectiveness) dashboards.
# MAGIC
# MAGIC ### What just happened
# MAGIC Last month's OEE numbers were 8 percentage points lower than the plant managers' gut sense. On investigation, the IT/OT team found that the downtime calculations were treating brief sensor dropouts as actual machine downtime, and the anomaly alerts were swamped by false positives. The COO wants this fixed before the next board review.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The COO wants OEE numbers that match plant reality. The plant managers want machine-level downtime reports that distinguish real downtime from sensor gaps. The reliability engineer wants statistical anomaly detection rather than fixed thresholds.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the senior data engineer on the OT analytics team. Rebuild the sessionization, downtime detection, and anomaly detection logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, LongType, DateType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, date

sensor_readings_schema = StructType([
    StructField("reading_id", LongType(), False),
    StructField("machine_id", IntegerType(), True),
    StructField("reading_ts", TimestampType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("spindle_rpm", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("ingest_ts", TimestampType(), True),
])

# Machine 401: continuous run with one true downtime gap
# Machine 402: brief sensor gap (NOT real downtime - readings still come, just sparse) followed by real downtime
# Machine 403: lots of small fluctuations including one anomaly value
readings_data = [
    # Machine 401
    (1, 401, datetime(2025, 7, 1, 9, 0),  2.1, 75.0, 1200, "running", datetime(2025, 7, 1, 9, 0, 5)),
    (2, 401, datetime(2025, 7, 1, 9, 10), 2.2, 76.0, 1200, "running", datetime(2025, 7, 1, 9, 10, 5)),
    (3, 401, datetime(2025, 7, 1, 9, 20), 2.0, 75.5, 1180, "running", datetime(2025, 7, 1, 9, 20, 5)),
    # Real downtime: status = stopped from 9:30 to 10:20
    (4, 401, datetime(2025, 7, 1, 9, 30), 0.0, 70.0, 0,    "stopped", datetime(2025, 7, 1, 9, 30, 5)),
    (5, 401, datetime(2025, 7, 1, 9, 40), 0.0, 68.0, 0,    "stopped", datetime(2025, 7, 1, 9, 40, 5)),
    (6, 401, datetime(2025, 7, 1, 9, 50), 0.0, 67.0, 0,    "stopped", datetime(2025, 7, 1, 9, 50, 5)),
    (7, 401, datetime(2025, 7, 1, 10, 0), 0.0, 65.0, 0,    "stopped", datetime(2025, 7, 1, 10, 0, 5)),
    (8, 401, datetime(2025, 7, 1, 10, 10),0.0, 65.0, 0,    "stopped", datetime(2025, 7, 1, 10, 10, 5)),
    (9, 401, datetime(2025, 7, 1, 10, 20),0.0, 66.0, 0,    "stopped", datetime(2025, 7, 1, 10, 20, 5)),
    (10,401, datetime(2025, 7, 1, 10, 30),2.0, 72.0, 1150, "running", datetime(2025, 7, 1, 10, 30, 5)),
    (11,401, datetime(2025, 7, 1, 10, 40),2.1, 74.0, 1180, "running", datetime(2025, 7, 1, 10, 40, 5)),
    # Machine 402
    (12,402, datetime(2025, 7, 1, 9, 0),  3.0, 80.0, 1500, "running", datetime(2025, 7, 1, 9, 0, 5)),
    (13,402, datetime(2025, 7, 1, 9, 10), 3.1, 81.0, 1510, "running", datetime(2025, 7, 1, 9, 10, 5)),
    # Sensor gap (no reading at 9:20) but next reading at 9:30 says running
    (14,402, datetime(2025, 7, 1, 9, 30), 3.0, 82.0, 1520, "running", datetime(2025, 7, 1, 9, 30, 5)),
    # Real downtime 10:00 to 10:20
    (15,402, datetime(2025, 7, 1, 9, 40), 3.0, 82.0, 1500, "running", datetime(2025, 7, 1, 9, 40, 5)),
    (16,402, datetime(2025, 7, 1, 9, 50), 3.1, 82.5, 1510, "running", datetime(2025, 7, 1, 9, 50, 5)),
    (17,402, datetime(2025, 7, 1, 10, 0), 0.0, 76.0, 0,    "stopped", datetime(2025, 7, 1, 10, 0, 5)),
    (18,402, datetime(2025, 7, 1, 10, 10),0.0, 74.0, 0,    "stopped", datetime(2025, 7, 1, 10, 10, 5)),
    (19,402, datetime(2025, 7, 1, 10, 20),0.0, 72.0, 0,    "stopped", datetime(2025, 7, 1, 10, 20, 5)),
    (20,402, datetime(2025, 7, 1, 10, 30),3.0, 80.0, 1500, "running", datetime(2025, 7, 1, 10, 30, 5)),
    # Machine 403 with a vibration anomaly
    (21,403, datetime(2025, 7, 1, 9, 0),  4.0, 85.0, 1800, "running", datetime(2025, 7, 1, 9, 0, 5)),
    (22,403, datetime(2025, 7, 1, 9, 10), 4.1, 86.0, 1810, "running", datetime(2025, 7, 1, 9, 10, 5)),
    (23,403, datetime(2025, 7, 1, 9, 20), 4.0, 85.5, 1800, "running", datetime(2025, 7, 1, 9, 20, 5)),
    (24,403, datetime(2025, 7, 1, 9, 30), 9.5, 88.0, 1850, "running", datetime(2025, 7, 1, 9, 30, 5)),  # anomaly!
    (25,403, datetime(2025, 7, 1, 9, 40), 4.0, 86.0, 1810, "running", datetime(2025, 7, 1, 9, 40, 5)),
    (26,403, datetime(2025, 7, 1, 9, 50), 4.1, 86.5, 1820, "running", datetime(2025, 7, 1, 9, 50, 5)),
    (27,403, datetime(2025, 7, 1, 10, 0), 4.0, 86.0, 1810, "running", datetime(2025, 7, 1, 10, 0, 5)),
    (28,403, datetime(2025, 7, 1, 10, 10),4.2, 87.0, 1830, "running", datetime(2025, 7, 1, 10, 10, 5)),
    # Late-arriving duplicate
    (24,403, datetime(2025, 7, 1, 9, 30), 9.5, 88.0, 1850, "running", datetime(2025, 7, 1, 10, 30, 0)),
]
readings = spark.createDataFrame(readings_data, sensor_readings_schema)
readings.createOrReplaceTempView("readings")

print(f"readings: {readings.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Clean up the sensor stream
# MAGIC
# MAGIC The `readings` table has duplicates by `reading_id`. Among duplicates, prefer:
# MAGIC 1. Status = "running" over "stopped" (running is the operational truth; stopped readings often come from sensor failure)
# MAGIC 2. Then latest ingest_ts
# MAGIC
# MAGIC Use a derived priority integer + ROW_NUMBER pattern.
# MAGIC
# MAGIC Output the deduplicated reading set (full columns). Sort by machine_id, reading_ts.

# COMMAND ----------

# Your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Identify each machine's continuous running periods
# MAGIC
# MAGIC From the deduplicated readings, define a "running session" as a contiguous run of `status = "running"` readings for a given machine. A machine can have multiple running sessions in a day, separated by stopped stretches.
# MAGIC
# MAGIC Output: `machine_id`, `running_session_id` (1, 2, 3 per machine), `session_start`, `session_end`, `reading_count`. Sort by machine_id, running_session_id.

# COMMAND ----------

# Your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Downtime period detection
# MAGIC
# MAGIC Mirror Task 2 but for "stopped" status. Each contiguous run of stopped status is one downtime period.
# MAGIC
# MAGIC Output: `machine_id`, `downtime_id`, `downtime_start`, `downtime_end`, `duration_minutes`. Sort by machine_id, downtime_id.
# MAGIC
# MAGIC Key requirement: distinguish a real downtime (multiple consecutive stopped readings) from a sensor blip (a single isolated stopped reading surrounded by running). Filter out downtime periods of duration < 10 minutes.

# COMMAND ----------

# Your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Statistical anomaly detection
# MAGIC
# MAGIC For each machine, flag readings where `vibration` is more than 3 standard deviations above the machine's trailing 30-reading mean — the 3-sigma rule for control-chart anomaly detection.
# MAGIC
# MAGIC Important: the trailing baseline must EXCLUDE the current row, otherwise the anomaly contaminates its own comparison and underflags.
# MAGIC
# MAGIC Output: `reading_id`, `machine_id`, `reading_ts`, `vibration`, `trailing_avg`, `trailing_stddev`, `is_anomaly` (boolean). Sort by machine_id, reading_ts. Show only flagged anomalies after sorting (filter `is_anomaly = true`).

# COMMAND ----------

# Your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · OEE-style availability calculation
# MAGIC
# MAGIC For each machine on 2025-07-01, compute:
# MAGIC - `scheduled_minutes`: assume 120 minutes (the data spans 9:00 to 10:50)
# MAGIC - `running_minutes`: total minutes across all running sessions (from Task 2)
# MAGIC - `downtime_minutes`: total real downtime (from Task 3, periods >= 10 min)
# MAGIC - `availability_pct`: 100 × running_minutes / scheduled_minutes
# MAGIC
# MAGIC Output: `machine_id`, `scheduled_minutes`, `running_minutes`, `downtime_minutes`, `availability_pct`. Sort by machine_id.

# COMMAND ----------

# Your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ### Topics this test covers
# MAGIC Stream cleanup, contiguous-period detection, statistical anomaly detection, and OEE-style aggregation. If you're stuck, the relevant material is from weeks 8-11 of the cohort.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## What done looks like
# MAGIC
# MAGIC - Task 1: Deduplicated readings with status priority + ingest_ts tiebreak.
# MAGIC - Task 2: Running sessions identified using gaps-and-islands.
# MAGIC - Task 3: Downtime periods identified, with sub-10-min noise filtered out.
# MAGIC - Task 4: Vibration anomalies flagged using 3-sigma rule.
# MAGIC - Task 5: OEE-style availability percentage computed per machine.
