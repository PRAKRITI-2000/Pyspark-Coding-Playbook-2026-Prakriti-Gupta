# Databricks notebook source
# MAGIC %md
# MAGIC # Test 06 · AxisOne Bank · Questions
# MAGIC
# MAGIC **Medium · ~75-90 min · 6 tasks**
# MAGIC
# MAGIC *Mumbai · Private Sector Bank · Listed · 65,000 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC AxisOne Bank is a 28-year-old private sector bank with 3,200 branches and 12 million retail customers. Risk and compliance functions run on a central data warehouse that ingests from 14 source systems. Regulators expect full audit trails on every customer attribute change.
# MAGIC
# MAGIC ### What just happened
# MAGIC RBI auditors flagged the customer dimension table in last week's review. The table overwrites old values when customer details change. As of today, no one can reconstruct what a customer's risk_segment was on the day a loan was approved six months ago. The audit team has 30 days to produce a remediated dimension with full history, or face an RBI directive.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The Chief Risk Officer wants SCD Type 2 implemented on customer dimension before next quarter's audit. The data platform lead wants it on Delta with proper time travel. The compliance head wants idempotent loads so a re-run of yesterday's job does not corrupt the table.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the senior data engineer on the customer dimension remediation. Build the SCD2 logic, demonstrate idempotency, and prove out time travel.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, BooleanType, LongType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date

# Current state of customer dimension (target table) - Day 1 load already happened
target_v1_schema = StructType([
    StructField("surrogate_key", LongType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("risk_segment", StringType(), True),
    StructField("effective_from", DateType(), True),
    StructField("effective_to", DateType(), True),
    StructField("is_current", BooleanType(), True),
])
target_v1_data = [
    (1, 101, "Aarav Sharma", "Mumbai",    "low",    date(2025, 1, 1), date(9999, 12, 31), True),
    (2, 102, "Priya Iyer",   "Bengaluru", "medium", date(2025, 1, 1), date(9999, 12, 31), True),
    (3, 103, "Rohan Kumar",  "Pune",      "low",    date(2025, 1, 1), date(9999, 12, 31), True),
    (4, 104, "Sneha Reddy",  "Hyderabad", "high",   date(2025, 1, 1), date(9999, 12, 31), True),
    (5, 105, "Karan Mehta",  "Delhi",     "medium", date(2025, 1, 1), date(9999, 12, 31), True),
]
customer_dim = spark.createDataFrame(target_v1_data, target_v1_schema)
customer_dim.createOrReplaceTempView("customer_dim")

# Incoming source data - Day 2 (today)
# customer_id 101: city changed Mumbai -> Pune (true change)
# customer_id 102: no change
# customer_id 103: risk_segment changed low -> medium (true change)
# customer_id 104: no change
# customer_id 105: no change
# customer_id 106: NEW customer
source_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("risk_segment", StringType(), True),
])
source_data = [
    (101, "Aarav Sharma", "Pune",      "low"),
    (102, "Priya Iyer",   "Bengaluru", "medium"),
    (103, "Rohan Kumar",  "Pune",      "medium"),
    (104, "Sneha Reddy",  "Hyderabad", "high"),
    (105, "Karan Mehta",  "Delhi",     "medium"),
    (106, "Anjali Bose",  "Kolkata",   "low"),
]
source_today = spark.createDataFrame(source_data, source_schema)
source_today.createOrReplaceTempView("source_today")

# Load date
load_date = date(2025, 7, 14)

print(f"customer_dim rows: {customer_dim.count()}, source_today rows: {source_today.count()}")
print(f"Load date for this run: {load_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Tag each source row by what changed
# MAGIC
# MAGIC Compare `source_today` to the CURRENT records (`is_current = true`) in `customer_dim`. Tag each source row with a `cdc_flag`:
# MAGIC - "I" (insert) if customer_id does not exist in customer_dim
# MAGIC - "U" (update) if customer_id exists but city OR risk_segment differs
# MAGIC - "N" (no change) if customer_id exists and all tracked attributes match
# MAGIC
# MAGIC Output columns: `customer_id`, `customer_name`, `city`, `risk_segment`, `cdc_flag`. Sort by customer_id.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Produce the post-load dimension state by hand
# MAGIC
# MAGIC Build the new state of `customer_dim` AFTER applying today's source. Without using MERGE, just produce the target DataFrame the table SHOULD look like after the load:
# MAGIC
# MAGIC - For "U" rows: close the old current row (set effective_to = load_date - 1, is_current = false) AND insert a new row (effective_from = load_date, effective_to = 9999-12-31, is_current = true).
# MAGIC - For "I" rows: insert a new row with effective_from = load_date.
# MAGIC - For "N" rows: leave the existing row untouched.
# MAGIC
# MAGIC New rows get new surrogate keys (continue numbering from current max).
# MAGIC
# MAGIC Output columns: full dim schema. Sort by customer_id, effective_from.

# COMMAND ----------

# Your PySpark solution here (this one is PySpark-first, SQL second)

# COMMAND ----------

# Your SQL solution here (using UNION ALL of closed + new + unchanged rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Key strategy for the dimension
# MAGIC
# MAGIC The CRO asks: "Why do we need surrogate keys when we have customer_id?"
# MAGIC
# MAGIC In a markdown cell below, answer in 4-5 lines max. Then in code:
# MAGIC
# MAGIC ### Part A
# MAGIC Generate surrogate keys using `monotonically_increasing_id()` for a small input DataFrame.
# MAGIC
# MAGIC ### Part B
# MAGIC Generate a stable hash-based surrogate key using `sha2(concat_ws('|', customer_id, effective_from), 256)` for the same input. Show both.
# MAGIC
# MAGIC ### Part C
# MAGIC Define a composite natural key as `customer_id + effective_from` and verify it is unique across the SCD2 dimension.

# COMMAND ----------

# MAGIC %md
# MAGIC **Your answer to "Why surrogate keys?" here.**

# COMMAND ----------

# Your code for Part A, B, C here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Same load, expressed in Delta SQL
# MAGIC
# MAGIC Re-implement Task 2 using Delta Lake MERGE for a target table called `customer_dim_delta` and a source called `source_today`. A single MERGE clause cannot both close an existing row AND insert a new version for the same key in one pass — design your SQL accordingly.
# MAGIC
# MAGIC You do not need to actually create the Delta table to run this - just write the MERGE statements correctly with comments explaining each step.
# MAGIC
# MAGIC (If you want to run it, the cell below sets up the Delta table.)

# COMMAND ----------

# Optional: actual Delta table setup
# Uncomment to run end-to-end

# customer_dim.write.format("delta").mode("overwrite").saveAsTable("customer_dim_delta")
# print("customer_dim_delta created.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your MERGE statement(s) here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Make the load safe to re-run
# MAGIC
# MAGIC The compliance head wants the SCD2 load to be IDEMPOTENT. Re-running today's job for the same load_date should NOT create duplicate version rows or close-then-reopen rows.
# MAGIC
# MAGIC In a markdown cell, explain the two main idempotency guards for an SCD2 MERGE pipeline.
# MAGIC
# MAGIC Then in code, demonstrate the guard: if the dimension already has a row with `effective_from = today's load_date` for a given customer, do NOT process that customer's update again.

# COMMAND ----------

# MAGIC %md
# MAGIC **Idempotency guards explanation here.**

# COMMAND ----------

# Your code demonstrating the guard here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · Answer the auditor's point-in-time question
# MAGIC
# MAGIC The auditor's specific question: "What was customer 103's risk_segment on July 1?"
# MAGIC
# MAGIC ### Part A
# MAGIC Without Delta time travel, answer using the SCD2 effective dates only.
# MAGIC
# MAGIC ### Part B
# MAGIC Write the Delta time travel SQL using `VERSION AS OF` and `TIMESTAMP AS OF` to answer the same question.
# MAGIC
# MAGIC ### Part C
# MAGIC Show `DESCRIBE HISTORY` and `RESTORE` syntax with comments on when each is used.

# COMMAND ----------

# Your Part A SQL or PySpark solution here

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Part B: Delta time travel
# MAGIC
# MAGIC -- Part C: DESCRIBE HISTORY and RESTORE

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## What done looks like
# MAGIC
# MAGIC - Task 1: CDC flags correctly tagged (I, U, N).
# MAGIC - Task 2: New dimension state with expired + new + unchanged rows assembled.
# MAGIC - Task 3: Surrogate key strategies demonstrated (monotonically_increasing_id, sha2 hash).
# MAGIC - Task 4: SCD2 load expressed in Delta MERGE syntax.
# MAGIC - Task 5: Idempotency guards explained and demonstrated.
# MAGIC - Task 6: Time travel answered three ways (effective dates, VERSION AS OF, TIMESTAMP AS OF).
