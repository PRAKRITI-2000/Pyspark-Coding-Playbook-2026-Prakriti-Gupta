# Databricks notebook source
# MAGIC %md
# MAGIC # Test 08 · StreamHaus · Questions
# MAGIC
# MAGIC **Medium · ~75-90 min · 5 tasks**
# MAGIC
# MAGIC *Mumbai · OTT Streaming · Series C · 1,800 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC StreamHaus runs an OTT platform with 22 million subscribers, content in 11 Indian languages, and 80,000 hours of catalogue. Engagement metrics drive content licensing decisions worth crores.
# MAGIC
# MAGIC ### What just happened
# MAGIC The content team renewed an expensive regional drama based on "session count" metrics. Two weeks after renewal, the analytics team realised the session-count logic was over-counting: every pause was being treated as a new session. The renewal decision is signed, but every future renewal needs the metric fixed before the next licensing review in three weeks.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The Chief Content Officer wants accurate session metrics. The VP Analytics wants a per-show pivot of engagement. The data platform lead wants the session logic to handle the 30-minute inactivity convention used industry-wide.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the data engineer rebuilding the engagement metrics layer. Sessionize correctly, pivot the engagement, and produce trailing-window views.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, LongType, DateType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, date

events_schema = StructType([
    StructField("event_id", LongType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("content_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
])
events_data = [
    # User 1: continuous viewing then 35-min gap then new session
    (1, 1, datetime(2025, 7, 1, 19, 0),  101, "play"),
    (2, 1, datetime(2025, 7, 1, 19, 10), 101, "play"),
    (3, 1, datetime(2025, 7, 1, 19, 25), 101, "play"),
    (4, 1, datetime(2025, 7, 1, 20, 0),  102, "play"),  # 35 min gap, new session
    (5, 1, datetime(2025, 7, 1, 20, 15), 102, "play"),
    # User 2: short session
    (6, 2, datetime(2025, 7, 1, 20, 30), 103, "play"),
    (7, 2, datetime(2025, 7, 1, 20, 45), 103, "play"),
    # User 3: three sessions in a day
    (8, 3, datetime(2025, 7, 1, 10, 0),  104, "play"),
    (9, 3, datetime(2025, 7, 1, 10, 20), 104, "play"),
    (10,3, datetime(2025, 7, 1, 14, 0),  105, "play"),  # gap > 30 min
    (11,3, datetime(2025, 7, 1, 14, 20), 105, "play"),
    (12,3, datetime(2025, 7, 1, 22, 0),  106, "play"),  # another gap
    # User 4: spread over multiple days
    (13,4, datetime(2025, 7, 1, 21, 0),  107, "play"),
    (14,4, datetime(2025, 7, 2, 8,  0),  108, "play"),  # next day, new session
    (15,4, datetime(2025, 7, 2, 8,  20), 108, "play"),
    (16,4, datetime(2025, 7, 3, 19, 0),  109, "play"),
]
events = spark.createDataFrame(events_data, events_schema)
events.createOrReplaceTempView("events")

content_schema = StructType([
    StructField("content_id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("category", StringType(), True),
    StructField("language", StringType(), True),
])
content_data = [
    (101, "Maya's Mumbai",         "drama",   "Hindi"),
    (102, "Detective Vakeel",      "thriller","Tamil"),
    (103, "Cooking with Anjali",   "food",    "Hindi"),
    (104, "Coastal Mysteries",     "drama",   "Malayalam"),
    (105, "Numbers Don't Lie",     "thriller","Hindi"),
    (106, "Twilight Trains",       "drama",   "Bengali"),
    (107, "Tech Tonic",            "comedy",  "Hindi"),
    (108, "Saturday Sessions",     "drama",   "Telugu"),
    (109, "Last Light",            "thriller","Marathi"),
]
content = spark.createDataFrame(content_data, content_schema)
content.createOrReplaceTempView("content")

print(f"events: {events.count()}, content: {content.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Assign a session ID to each event
# MAGIC
# MAGIC Industry convention: a session ends after 30 minutes of inactivity. The next event after that gap starts a new session.
# MAGIC
# MAGIC Assign a `session_id` per user. session_id starts at 1 for each user's first session, increments when a 30-min gap is detected.
# MAGIC
# MAGIC Output columns: `event_id`, `user_id`, `event_ts`, `content_id`, `session_id`. Sort by user_id, event_ts.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Session summary
# MAGIC
# MAGIC From the sessionized output, compute per-session metrics:
# MAGIC - session_start_ts, session_end_ts
# MAGIC - event_count
# MAGIC - duration_minutes
# MAGIC - distinct_content_count
# MAGIC
# MAGIC Output columns: `user_id`, `session_id`, `session_start_ts`, `session_end_ts`, `event_count`, `duration_minutes`, `distinct_content_count`. Sort by user_id, session_id.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Daily events broken out by category, one column per category
# MAGIC
# MAGIC The VP Analytics wants daily event counts broken out by content category in columns.
# MAGIC
# MAGIC Output: one row per (date), columns for `drama`, `thriller`, `food`, `comedy`. Values = event count.
# MAGIC
# MAGIC Sort by date.
# MAGIC
# MAGIC Solve in SQL first using `PIVOT`, then PySpark using `.groupBy().pivot()`.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Reshape the previous matrix back to rows
# MAGIC
# MAGIC Take the pivoted result from Task 3 and reverse it: each row becomes (date, category, event_count).
# MAGIC
# MAGIC Use SQL `UNPIVOT` syntax (or `stack()` if your Spark version does not support UNPIVOT) and the PySpark `stack` expression.
# MAGIC
# MAGIC Output columns: `date`, `category`, `event_count`. Sort by date, category.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Trailing 7-day event count per user
# MAGIC
# MAGIC For each (user_id, event_date), compute the count of events in the trailing 7-day window (current day plus the 6 previous days).
# MAGIC
# MAGIC Use `RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW` (or the PySpark equivalent with `rangeBetween` on a numeric date column).
# MAGIC
# MAGIC Output columns: `user_id`, `event_date`, `events_today`, `events_trailing_7d`. Sort by user_id, event_date.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## What done looks like
# MAGIC
# MAGIC - Task 1: Session IDs correctly assigned using 30-min inactivity rule.
# MAGIC - Task 2: Per-session metrics (start, end, count, duration, distinct content).
# MAGIC - Task 3: PIVOT producing one row per date with category columns.
# MAGIC - Task 4: UNPIVOT reversing the pivot.
# MAGIC - Task 5: Trailing 7-day window using rangeBetween.
