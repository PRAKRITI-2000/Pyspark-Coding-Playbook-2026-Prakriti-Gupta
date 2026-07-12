# Databricks notebook source
# MAGIC %md
# MAGIC # Test 12 · NorthBeacon Ads · Questions
# MAGIC
# MAGIC **Hard · ~120 min · 6 tasks**
# MAGIC
# MAGIC *Gurugram · Ad-tech · Series D · 2,800 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC NorthBeacon runs a programmatic ad exchange handling 12 billion ad impressions a day across 14,000 advertisers. The pipeline joins impression streams with advertiser metadata. Five advertisers account for over 40% of impressions; the other 14,000 account for the rest. Classic data skew.
# MAGIC
# MAGIC ### What just happened
# MAGIC The nightly attribution job has been running 4x slower than its SLA for two weeks. The platform team's investigation shows one stage running for 90 minutes while every other executor finished in 8 minutes. A single Reliance-scale advertiser is monopolising a partition. The CTO wants the salting fix in place before the next campaign launch.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The CTO wants the attribution pipeline back inside its SLA. The data platform lead wants partitioning vs bucketing decisions documented for future tables. The senior engineer assigned to mentor you wants you to read execution plans correctly.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the senior data engineer pulled in to fix the skew, document the right partitioning strategy, and demonstrate fluency with `explain()` and Delta table tuning.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data
# MAGIC
# MAGIC In real production this would be billions of rows; here it's enough to demonstrate the patterns.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, LongType, DateType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, date

# Heavily skewed impressions: advertiser 1 has 60 rows, others have 1-3 each
impressions_rows = []
impression_id = 100001
for _ in range(60):
    impressions_rows.append((impression_id, 1, datetime(2025, 7, 1, 10, 0), 1.2))
    impression_id += 1
for adv_id in range(2, 21):
    for _ in range(2):
        impressions_rows.append((impression_id, adv_id, datetime(2025, 7, 1, 10, 30), 0.5 + adv_id * 0.1))
        impression_id += 1

impressions_schema = StructType([
    StructField("impression_id", LongType(), False),
    StructField("advertiser_id", IntegerType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("bid_amount", DoubleType(), True),
])
impressions = spark.createDataFrame(impressions_rows, impressions_schema)
impressions.createOrReplaceTempView("impressions")

advertisers_rows = [(i, f"Advertiser_{i}", "active" if i < 19 else "inactive") for i in range(1, 21)]
advertisers_schema = StructType([
    StructField("advertiser_id", IntegerType(), False),
    StructField("advertiser_name", StringType(), True),
    StructField("status", StringType(), True),
])
advertisers = spark.createDataFrame(advertisers_rows, advertisers_schema)
advertisers.createOrReplaceTempView("advertisers")

# Campaign metrics over multiple days for windowed queries
campaign_metrics_rows = []
for day_offset in range(15):
    for adv_id in range(1, 11):
        campaign_metrics_rows.append((
            adv_id,
            date(2025, 7, 1) + (date(2025, 7, day_offset + 1) - date(2025, 7, 1)),
            int(50000 + (adv_id * 1000) + (day_offset * 500)),
            float(15000 + adv_id * 200 + day_offset * 100),
        ))
campaign_metrics_schema = StructType([
    StructField("advertiser_id", IntegerType(), True),
    StructField("metric_date", DateType(), True),
    StructField("impressions", IntegerType(), True),
    StructField("spend", DoubleType(), True),
])
campaign_metrics = spark.createDataFrame(campaign_metrics_rows, campaign_metrics_schema)
campaign_metrics.createOrReplaceTempView("campaign_metrics")

print(f"impressions: {impressions.count()} (skewed: advertiser 1 has 60, others 2 each)")
print(f"advertisers: {advertisers.count()}, campaign_metrics: {campaign_metrics.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Identify the skew
# MAGIC
# MAGIC Compute impressions per advertiser. Show the percentage each advertiser holds of the total.
# MAGIC
# MAGIC Output: `advertiser_id`, `impression_count`, `pct_of_total`. Sort by impression_count descending.
# MAGIC
# MAGIC In a comment, identify which advertiser is the hot key and how this would cripple a JOIN.

# COMMAND ----------

# Your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Make the impression-advertiser join scale despite the skew
# MAGIC
# MAGIC Apply the standard fix for a skewed join so the advertiser_id = 1 hot key is redistributed across multiple partitions instead of monopolising one. Use a redistribution factor of 4 (in production this would be 8 to 32 depending on severity).
# MAGIC
# MAGIC The result must be functionally equivalent to a plain join — same row count, same values.
# MAGIC
# MAGIC Output: `impression_id`, `advertiser_id`, `advertiser_name`, `bid_amount`. Sort by impression_id.

# COMMAND ----------

# Your salting solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Adjust partition count, two ways
# MAGIC
# MAGIC ### Part A
# MAGIC Use `.repartition(8)` to increase partition count.
# MAGIC ### Part B
# MAGIC Use `.coalesce(2)` to decrease partition count.
# MAGIC
# MAGIC For both, print `getNumPartitions()` before and after.
# MAGIC
# MAGIC In a comment, explain:
# MAGIC - When you use repartition (and what it does to data movement)
# MAGIC - When you use coalesce (and how it differs)
# MAGIC - Why coalesce can lead to skew

# COMMAND ----------

# Your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Reading the execution plan
# MAGIC
# MAGIC Take two query plans:
# MAGIC
# MAGIC ### Part A
# MAGIC Plain JOIN of impressions and advertisers. Call `.explain(True)` and show the output.
# MAGIC
# MAGIC ### Part B
# MAGIC Same JOIN with `F.broadcast(advertisers)`. Call `.explain(True)`.
# MAGIC
# MAGIC In a markdown cell, point out which operator changed (SortMergeJoin → BroadcastHashJoin) and what the consequences are for shuffle.

# COMMAND ----------

# Part A: plain join explain

# COMMAND ----------

# Part B: broadcast join explain

# COMMAND ----------

# MAGIC %md
# MAGIC **Your observations on the plan diff here.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Per-advertiser daily metrics with trailing and rolling views
# MAGIC
# MAGIC For each advertiser, for each metric_date in campaign_metrics, compute:
# MAGIC - Daily impressions
# MAGIC - 7-day trailing total impressions
# MAGIC - 7-day trailing avg spend
# MAGIC - Rank of this day's impressions within the advertiser's last 30 days
# MAGIC
# MAGIC Use a date-as-integer trick for rangeBetween on date columns.
# MAGIC
# MAGIC Output columns: `advertiser_id`, `metric_date`, `daily_impressions`, `trailing_7d_impressions`, `trailing_7d_avg_spend`, `rank_in_last_30d`. Sort by advertiser_id, metric_date.

# COMMAND ----------

# Your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · Storage layout choices for the new attribution table
# MAGIC
# MAGIC The new `attribution_facts` table will have these key columns:
# MAGIC - `impression_id` (primary key, high cardinality)
# MAGIC - `advertiser_id` (~14,000 distinct, very common filter)
# MAGIC - `event_date` (~365 distinct per year, very common filter)
# MAGIC - `bid_amount`
# MAGIC - 100+ other columns
# MAGIC
# MAGIC Delta supports three layout strategies: directory-level partitioning, hash bucketing, and intra-file clustering via Z-Order.
# MAGIC
# MAGIC ### Part A
# MAGIC Write the Delta CREATE TABLE for `attribution_facts` with the right partitioning choice for this workload.
# MAGIC
# MAGIC ### Part B
# MAGIC Write the post-write OPTIMIZE statement that helps the high-cardinality filter column.
# MAGIC
# MAGIC ### Part C
# MAGIC Show the hash bucketing syntax as a comparison example, and explain when it would be a better choice than the partitioning + Part B approach.
# MAGIC
# MAGIC ### Part D
# MAGIC Explain why partitioning on `advertiser_id` would be a catastrophe.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Part A: PARTITIONED BY example with comments
# MAGIC
# MAGIC -- Part B: CLUSTERED BY example with comments
# MAGIC
# MAGIC -- Part C: OPTIMIZE ZORDER BY example with comments

# COMMAND ----------

# MAGIC %md
# MAGIC **Part D: when to partition vs Z-ORDER, with reasoning.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Topics this test covers
# MAGIC Skew handling, partition control, plan reading, and Delta storage layout decisions. If you're stuck, the relevant material is from weeks 9-11 of the cohort.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## What done looks like
# MAGIC
# MAGIC - Task 1: Hot key identified with percentage of total.
# MAGIC - Task 2: Salted join producing the same result as the plain join.
# MAGIC - Task 3: repartition and coalesce demonstrated with partition count check.
# MAGIC - Task 4: Plan diff between SortMergeJoin and BroadcastHashJoin observed and explained.
# MAGIC - Task 5: Multi-window query with trailing 7-day and rolling 30-day rank.
# MAGIC - Task 6: Delta partitioning, bucketing, and Z-ORDER syntax with use-case guidance.
