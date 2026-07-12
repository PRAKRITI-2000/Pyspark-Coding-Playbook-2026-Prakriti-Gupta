# Databricks notebook source
# MAGIC %md
# MAGIC # Test 09 · Surakshit Insurance · Questions
# MAGIC
# MAGIC **Medium · ~75-90 min · 6 tasks**
# MAGIC
# MAGIC *Chennai · Life Insurance · Listed · 14,000 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC Surakshit Insurance is a 35-year-old life insurance company with two million policyholders. Sales runs through a four-level agent network: head office, zonal managers, branch managers, agents. Compliance audits require traceable agent hierarchies and clean reconciliation between two parallel claims systems.
# MAGIC
# MAGIC ### What just happened
# MAGIC IRDAI sent a circular asking insurers to demonstrate their agent hierarchy is correctly maintained, with every agent traceable up to the head office. Separately, the claims integration team migrated half the historical claims to a new system, but a chunk of records appear in both. The audit team is sweating.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The Chief Compliance Officer wants the agent hierarchy view by month-end. The Head of Claims wants a clean reconciliation of old vs new claim records. Both are blocked on data engineering.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the data engineer pulled in for the compliance push. Build the recursive hierarchy traversal, demonstrate SCD Type 1 in-place updates for static reference data, and handle the claims set operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql import functions as F
from datetime import date

# Agent hierarchy: 4 levels deep
agents_schema = StructType([
    StructField("agent_id", IntegerType(), False),
    StructField("agent_name", StringType(), True),
    StructField("manager_id", IntegerType(), True),
    StructField("level", StringType(), True),
])
agents_data = [
    (1, "HO Mumbai",         None, "HO"),
    (2, "Zonal North",       1,    "Zonal"),
    (3, "Zonal South",       1,    "Zonal"),
    (4, "Zonal West",        1,    "Zonal"),
    (5, "Branch Delhi",      2,    "Branch"),
    (6, "Branch Gurgaon",    2,    "Branch"),
    (7, "Branch Chennai",    3,    "Branch"),
    (8, "Branch Bengaluru",  3,    "Branch"),
    (9, "Branch Pune",       4,    "Branch"),
    (10,"Agent Sharma",      5,    "Agent"),
    (11,"Agent Verma",       5,    "Agent"),
    (12,"Agent Kapoor",      6,    "Agent"),
    (13,"Agent Iyer",        7,    "Agent"),
    (14,"Agent Reddy",       8,    "Agent"),
    (15,"Agent Shah",        9,    "Agent"),
    (16,"Agent Patel",       9,    "Agent"),
]
agents = spark.createDataFrame(agents_data, agents_schema)
agents.createOrReplaceTempView("agents")

# Old claims system
claims_old_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("policy_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("claim_date", DateType(), True),
    StructField("status", StringType(), True),
])
claims_old_data = [
    ("CLM001", 5001, 150000.0, date(2024, 3, 10), "approved"),
    ("CLM002", 5002, 220000.0, date(2024, 5, 15), "approved"),
    ("CLM003", 5003, 80000.0,  date(2024, 6, 1),  "rejected"),
    ("CLM004", 5004, 300000.0, date(2024, 7, 20), "pending"),
    ("CLM005", 5005, 175000.0, date(2024, 8, 5),  "approved"),
    ("CLM006", 5006, 95000.0,  date(2024, 9, 12), "approved"),
]
claims_old = spark.createDataFrame(claims_old_data, claims_old_schema)
claims_old.createOrReplaceTempView("claims_old")

# New claims system - some overlap with old
claims_new_data = [
    ("CLM004", 5004, 300000.0, date(2024, 7, 20), "approved"),  # status updated in new system
    ("CLM005", 5005, 175000.0, date(2024, 8, 5),  "approved"),  # identical
    ("CLM006", 5006, 95000.0,  date(2024, 9, 12), "approved"),  # identical
    ("CLM007", 5007, 250000.0, date(2024,10, 8),  "approved"),  # new only
    ("CLM008", 5008, 110000.0, date(2024,11, 22), "approved"),  # new only
    ("CLM009", 5009, 425000.0, date(2024,12, 3),  "pending"),   # new only
]
claims_new = spark.createDataFrame(claims_new_data, claims_old_schema)
claims_new.createOrReplaceTempView("claims_new")

# Policy reference table for SCD Type 1 demo
policies_schema = StructType([
    StructField("policy_id", IntegerType(), False),
    StructField("policy_type", StringType(), True),
    StructField("sum_assured", DoubleType(), True),
    StructField("status", StringType(), True),
])
policies_data = [
    (5001, "term_life",     1000000.0, "active"),
    (5002, "endowment",     500000.0,  "active"),
    (5003, "term_life",     2000000.0, "lapsed"),
    (5004, "ulip",          750000.0,  "active"),
    (5005, "endowment",     400000.0,  "active"),
]
policies = spark.createDataFrame(policies_data, policies_schema)
policies.createOrReplaceTempView("policies")

print(f"agents: {agents.count()}, claims_old: {claims_old.count()}, claims_new: {claims_new.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Walk the agent hierarchy in SQL
# MAGIC
# MAGIC Build a recursive CTE that walks the agent hierarchy from each leaf (an agent at level "Agent") all the way up to HO. For each agent, return the path from HO to that agent as a slash-separated string, and the depth.
# MAGIC
# MAGIC Output columns: `agent_id`, `agent_name`, `level`, `depth`, `path_from_ho`. Sort by depth, then agent_id.
# MAGIC
# MAGIC Spark supports recursive CTEs via `WITH RECURSIVE` (Databricks Runtime 14.3+). If your environment does not support it, write the CTE anyway with a comment; the iterative PySpark equivalent in Task 2 will run.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Same hierarchy walk in PySpark
# MAGIC
# MAGIC In environments without recursive CTE support, the alternative is an iterative join loop. Start with the top of the tree (HO, depth 0), iteratively join to find children at depth 1, then 2, and so on, until no new rows are added.
# MAGIC
# MAGIC Implement the iteration in PySpark. Output columns: same as Task 1. Sort by depth, agent_id.

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Overwrite-style update to the policies table
# MAGIC
# MAGIC SCD Type 1: overwrite old values, no history retained. Use for slowly-changing reference data where history is not needed.
# MAGIC
# MAGIC The `policies` table needs updates from the source:
# MAGIC - policy_id 5001: sum_assured changed from 1,000,000 to 1,200,000
# MAGIC - policy_id 5003: status changed from "lapsed" to "revived"
# MAGIC - policy_id 5006: new policy, term_life, 800,000, active
# MAGIC
# MAGIC Apply the SCD Type 1 update using Delta MERGE syntax (write the SQL with comments; you do not need to actually run against a Delta table).
# MAGIC
# MAGIC Then write the equivalent PySpark using a JOIN + COALESCE pattern (since pure DataFrames lack MERGE).

# COMMAND ----------

# Source updates DataFrame
policy_updates_data = [
    (5001, "term_life", 1200000.0, "active"),
    (5003, "term_life", 2000000.0, "revived"),
    (5006, "term_life", 800000.0,  "active"),
]
policy_updates = spark.createDataFrame(policy_updates_data, policies.schema)
policy_updates.createOrReplaceTempView("policy_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your SCD Type 1 MERGE syntax here (with comments)

# COMMAND ----------

# Your PySpark JOIN-based SCD1 implementation here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · UNION vs UNION ALL
# MAGIC
# MAGIC Combine claims_old and claims_new into a single dataset.
# MAGIC
# MAGIC ### Part A
# MAGIC Use `UNION ALL` and count the result.
# MAGIC
# MAGIC ### Part B
# MAGIC Use `UNION` (which deduplicates) and count the result.
# MAGIC
# MAGIC ### Part C
# MAGIC In a comment, note why UNION ALL is usually preferred even when you want dedup (and how to dedup more efficiently).

# COMMAND ----------

# Your SQL Part A and B here

# COMMAND ----------

# Your PySpark Part A and B here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Reconcile the two claims systems
# MAGIC
# MAGIC ### Part A
# MAGIC Find claim_ids present in BOTH systems using `INTERSECT`.
# MAGIC
# MAGIC ### Part B
# MAGIC Find claim_ids present in the OLD system but NOT in the NEW system using `EXCEPT`.
# MAGIC
# MAGIC ### Part C
# MAGIC For the claim_ids in both systems, find those where the STATUS differs between the two systems. (Use full-row comparison or join.)
# MAGIC
# MAGIC Solve in SQL first, then PySpark using `.intersect()`, `.exceptAll()`, etc.

# COMMAND ----------

# Your SQL Part A, B, C here

# COMMAND ----------

# Your PySpark Part A, B, C here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · Lookup the sum_assured per claim, two ways
# MAGIC
# MAGIC For each claim in claims_new, show the corresponding policy's sum_assured. Solve using a CORRELATED SUBQUERY in the SELECT clause.
# MAGIC
# MAGIC Then solve the same problem using a JOIN.
# MAGIC
# MAGIC In a comment, note when correlated subqueries are appropriate in Spark (and when they cripple performance).
# MAGIC
# MAGIC Output columns: `claim_id`, `policy_id`, `amount`, `sum_assured`. Sort by claim_id.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your correlated subquery solution here

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Your JOIN solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## What done looks like
# MAGIC
# MAGIC - Task 1: Recursive CTE walks the hierarchy with depth and path.
# MAGIC - Task 2: Iterative PySpark loop produces the same result.
# MAGIC - Task 3: SCD Type 1 update via MERGE (SQL) and JOIN-based pattern (PySpark).
# MAGIC - Task 4: UNION vs UNION ALL counts compared, with note on preference.
# MAGIC - Task 5: INTERSECT, EXCEPT, and status-diff identification.
# MAGIC - Task 6: Correlated subquery and JOIN equivalents with performance note.
