# Databricks notebook source
# MAGIC %md
# MAGIC # Test 10 · PaySetu · Questions
# MAGIC
# MAGIC **Medium · ~75-90 min · 5 tasks**
# MAGIC
# MAGIC *Bengaluru · FinTech Payments · Series C · 1,200 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC PaySetu processes UPI and card payments for 1.5 million merchants across India. The merchant onboarding system writes to a customer-merchant relationship table, and risk-scoring updates come in from multiple downstream systems on different schedules. The data lands with composite natural keys, frequent updates, and occasional late arrivals.
# MAGIC
# MAGIC ### What just happened
# MAGIC RBI's Payment Aggregator licence renewal review starts in 60 days. The risk team's score history is critical evidence, but the current implementation keys SCD2 on customer_id alone, missing the fact that a customer can be a merchant under multiple GST identities (the actual natural key is composite). The risk team also found cases where a status update for July 1 arrived on July 5, and the existing pipeline incorrectly stamped it as a July 5 change.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The Head of Risk wants the SCD2 implementation re-keyed correctly. The compliance lead wants late-arriving updates handled without rewriting history. The data platform lead wants priority-based deduplication for transaction-status snapshots.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the data engineer rebuilding the SCD2 pipeline. Get the composite key right, handle late arrivals, and implement priority dedup.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, BooleanType, LongType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date, timedelta

# Current state: merchant-customer dimension keyed on (customer_id, gst_id)
merchant_dim_schema = StructType([
    StructField("sk", LongType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("gst_id", StringType(), False),
    StructField("business_name", StringType(), True),
    StructField("risk_score", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("effective_from", DateType(), True),
    StructField("effective_to", DateType(), True),
    StructField("is_current", BooleanType(), True),
])
merchant_dim_data = [
    (1, 101, "29ABCDE1234F1Z5", "Mehta Stores",    65, "retail",      date(2025, 1, 1), date(9999, 12, 31), True),
    (2, 101, "27ABCDE1234F1Z5", "Mehta Online",    70, "ecommerce",   date(2025, 1, 1), date(9999, 12, 31), True),
    (3, 102, "07ABCDE5678F1Z6", "Kapoor Traders",  55, "wholesale",   date(2025, 1, 1), date(9999, 12, 31), True),
    (4, 103, "06ABCDE9012F1Z7", "Iyer Foods",      45, "restaurant",  date(2025, 1, 1), date(9999, 12, 31), True),
    (5, 104, "33ABCDE3456F1Z8", "Reddy Services",  60, "services",    date(2025, 1, 1), date(9999, 12, 31), True),
]
merchant_dim = spark.createDataFrame(merchant_dim_data, merchant_dim_schema)
merchant_dim.createOrReplaceTempView("merchant_dim")

# Incoming source with composite key changes
source_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("gst_id", StringType(), False),
    StructField("business_name", StringType(), True),
    StructField("risk_score", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("source_event_date", DateType(), True),
])
source_data = [
    # customer 101 has TWO GST IDs - they should be tracked independently
    (101, "29ABCDE1234F1Z5", "Mehta Stores",    72, "retail",      date(2025, 7, 14)),  # risk score changed
    (101, "27ABCDE1234F1Z5", "Mehta Online",    70, "ecommerce",   date(2025, 7, 14)),  # unchanged
    (102, "07ABCDE5678F1Z6", "Kapoor Traders",  55, "wholesale",   date(2025, 7, 14)),  # unchanged
    (103, "06ABCDE9012F1Z7", "Iyer Foods",      52, "restaurant",  date(2025, 7, 14)),  # risk score changed
    (104, "33ABCDE3456F1Z8", "Reddy Services",  60, "services",    date(2025, 7, 14)),  # unchanged
    (105, "21ABCDE7890F1Z9", "Singh Enterprises",75,"services",    date(2025, 7, 14)),  # new merchant
    # Late arrival: a July 1 change that only arrived now
    (102, "07ABCDE5678F1Z6", "Kapoor Traders",  58, "wholesale",   date(2025, 7, 1)),   # late!
]
source_today = spark.createDataFrame(source_data, source_schema)
source_today.createOrReplaceTempView("source_today")

load_date = date(2025, 7, 14)

# Transaction status updates (for dedup with priority task)
tx_updates_schema = StructType([
    StructField("txn_id", LongType(), False),
    StructField("status", StringType(), True),
    StructField("update_ts", DateType(), True),
])
tx_updates_data = [
    (9001, "pending",   date(2025, 7, 14)),
    (9001, "processing",date(2025, 7, 14)),
    (9001, "completed", date(2025, 7, 14)),
    (9002, "pending",   date(2025, 7, 14)),
    (9002, "failed",    date(2025, 7, 14)),
    (9003, "pending",   date(2025, 7, 14)),
    (9003, "processing",date(2025, 7, 14)),
    (9003, "processing",date(2025, 7, 14)),
    (9004, "completed", date(2025, 7, 14)),
    (9005, "pending",   date(2025, 7, 14)),
]
tx_updates = spark.createDataFrame(tx_updates_data, tx_updates_schema)
tx_updates.createOrReplaceTempView("tx_updates")

print(f"merchant_dim: {merchant_dim.count()}, source_today: {source_today.count()}, tx_updates: {tx_updates.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Compare today's source against the current dimension
# MAGIC
# MAGIC Tag each source row with cdc_flag ('I', 'U', 'N') comparing against the CURRENT slice of merchant_dim.
# MAGIC
# MAGIC The natural key is composite: `(customer_id, gst_id)`. Joining on customer_id alone matches WRONG rows.
# MAGIC
# MAGIC Output columns: `customer_id`, `gst_id`, `business_name`, `risk_score`, `category`, `cdc_flag`. Sort by customer_id, gst_id.
# MAGIC
# MAGIC For this task, ignore the late-arriving row (the one with source_event_date != load_date). It will be handled in Task 3.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Apply today's changes to the Delta dimension
# MAGIC
# MAGIC Write the two-step Delta MERGE (expire + insert) on the composite key `(customer_id, gst_id)`.
# MAGIC
# MAGIC Pure SQL with comments. You do not need to actually create the Delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your MERGE statement(s) here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Late-arriving updates
# MAGIC
# MAGIC The source contains one row with `source_event_date = 2025-07-01` that arrived today. The correct SCD2 representation:
# MAGIC - The version that was current from 2025-01-01 to 2025-06-30 is unchanged.
# MAGIC - The late-arriving change applies effective 2025-07-01: close out the old current row at 2025-06-30, insert a new row with effective_from = 2025-07-01.
# MAGIC - The CURRENT row continues from 2025-07-01 to 9999-12-31.
# MAGIC
# MAGIC Late arrivals are NOT inserted with effective_from = load_date. They are inserted with effective_from = source_event_date.
# MAGIC
# MAGIC Implement this handling for the one late-arriving row (customer_id = 102, gst_id = "07ABCDE5678F1Z6", source_event_date = 2025-07-01, risk_score = 58).
# MAGIC
# MAGIC Write PySpark code that:
# MAGIC 1. Detects the late arrival (source_event_date < load_date).
# MAGIC 2. Closes the existing current row at (source_event_date - 1 day).
# MAGIC 3. Inserts the new row with effective_from = source_event_date, effective_to = 9999-12-31, is_current = true.
# MAGIC
# MAGIC Show the resulting merchant_dim state for customer_id = 102.

# COMMAND ----------

# Your PySpark code here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Deduplicate transaction status updates
# MAGIC
# MAGIC The `tx_updates` table has multiple status updates per txn_id, all on the same day. The desired single row per txn_id should reflect the FURTHEST-PROGRESSED status, by this priority order:
# MAGIC
# MAGIC - `completed` > `failed` > `processing` > `pending`
# MAGIC
# MAGIC This is NOT a timestamp-based dedup. The order is a business priority, not a recency order.
# MAGIC
# MAGIC Implement the dedup so each txn_id ends up with the row reflecting its furthest-progressed status.
# MAGIC
# MAGIC Output columns: `txn_id`, `status`. Sort by txn_id.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Month-end position snapshots
# MAGIC
# MAGIC The compliance team needs: "What was each merchant's risk_score on the LAST DAY of each month from Jan to Jul 2025?"
# MAGIC
# MAGIC Pull the SCD2 version that was current on each month-end date. Months where the merchant did not yet exist should be omitted.
# MAGIC
# MAGIC Use the merchant_dim AFTER all of today's changes (Task 2 + Task 3) are applied. For this task, assume the post-update state is loaded into a view called `merchant_dim_v2`. The setup cell below populates it with the expected end-state.
# MAGIC
# MAGIC Output columns: `customer_id`, `gst_id`, `business_name`, `snapshot_date`, `risk_score`. Sort by customer_id, gst_id, snapshot_date.

# COMMAND ----------

# Build the end-state merchant_dim_v2 for this task
end_state_data = [
    # customer 101, gst 29...: changed today
    (1,  101, "29ABCDE1234F1Z5", "Mehta Stores",    65, "retail",      date(2025, 1, 1),  date(2025, 7, 13), False),
    (6,  101, "29ABCDE1234F1Z5", "Mehta Stores",    72, "retail",      date(2025, 7, 14), date(9999, 12, 31), True),
    # customer 101, gst 27...: unchanged
    (2,  101, "27ABCDE1234F1Z5", "Mehta Online",    70, "ecommerce",   date(2025, 1, 1),  date(9999, 12, 31), True),
    # customer 102, gst 07...: late-arriving change effective July 1
    (3,  102, "07ABCDE5678F1Z6", "Kapoor Traders",  55, "wholesale",   date(2025, 1, 1),  date(2025, 6, 30), False),
    (7,  102, "07ABCDE5678F1Z6", "Kapoor Traders",  58, "wholesale",   date(2025, 7, 1),  date(9999, 12, 31), True),
    # customer 103: changed today
    (4,  103, "06ABCDE9012F1Z7", "Iyer Foods",      45, "restaurant",  date(2025, 1, 1),  date(2025, 7, 13), False),
    (8,  103, "06ABCDE9012F1Z7", "Iyer Foods",      52, "restaurant",  date(2025, 7, 14), date(9999, 12, 31), True),
    # customer 104: unchanged
    (5,  104, "33ABCDE3456F1Z8", "Reddy Services",  60, "services",    date(2025, 1, 1),  date(9999, 12, 31), True),
    # customer 105: new today
    (9,  105, "21ABCDE7890F1Z9", "Singh Enterprises",75,"services",    date(2025, 7, 14), date(9999, 12, 31), True),
]
merchant_dim_v2 = spark.createDataFrame(end_state_data, merchant_dim_schema)
merchant_dim_v2.createOrReplaceTempView("merchant_dim_v2")
merchant_dim_v2.orderBy("customer_id", "gst_id", "effective_from").show(truncate=False)

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
# MAGIC - Task 1: CDC flags computed on composite key, ignoring late arrivals.
# MAGIC - Task 2: SCD2 MERGE written for composite key (two-step pattern).
# MAGIC - Task 3: Late arrival inserted with source_event_date, not load_date.
# MAGIC - Task 4: Priority dedup using CASE-derived integer ranking.
# MAGIC - Task 5: Monthly snapshot extracted from SCD2 history.
