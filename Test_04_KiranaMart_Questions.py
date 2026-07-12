# Databricks notebook source
# MAGIC %md
# MAGIC # Test 04 · Kirana Mart · Questions
# MAGIC
# MAGIC **Easy · ~30-40 min · 5 tasks**
# MAGIC
# MAGIC *Pune · Retail Grocery · Private · 8,500 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC Kirana Mart runs 340 physical stores across Maharashtra, Gujarat, and Karnataka. Stock-keeping units in the tens of thousands. Inventory turns vary wildly between fast-moving consumer goods and seasonal items. POS, warehouse, and supplier systems each speak different languages.
# MAGIC
# MAGIC ### What just happened
# MAGIC The CFO walked into the ops review last week and asked a simple question: "How much stock are we holding that has never sold?" Nobody had an answer. The warehouse team thinks one number, the merchandising team thinks another, and the supplier reconciliation team thinks both are wrong. Three SKU lists exist. None of them match.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The CFO wants the working capital tied up in dead stock. The COO wants to know which suppliers are sending SKUs that never move. The merchandising head wants to know what is in warehouses but not on shelves.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the data engineer pulled in to reconcile the three SKU lists and answer these questions for the next ops review.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql import functions as F
from datetime import date

warehouse_schema = StructType([
    StructField("sku", StringType(), False),
    StructField("warehouse_qty", IntegerType(), True),
    StructField("last_received_date", DateType(), True),
])
warehouse_data = [
    ("SKU001", 120, date(2025, 5, 15)),
    ("SKU002", 45,  date(2025, 6, 1)),
    ("SKU003", 80,  date(2025, 4, 20)),
    ("SKU004", 0,   date(2025, 1, 10)),
    ("SKU005", 200, date(2025, 6, 25)),
    ("SKU006", 60,  date(2025, 3, 5)),
    ("SKU009", 30,  date(2025, 5, 30)),
    ("SKU010", 150, date(2025, 2, 28)),
    ("SKU011", 90,  date(2025, 6, 10)),
    ("SKU013", 25,  date(2025, 4, 1)),
]
warehouse = spark.createDataFrame(warehouse_data, warehouse_schema)
warehouse.createOrReplaceTempView("warehouse")

store_sales_schema = StructType([
    StructField("sku", StringType(), False),
    StructField("units_sold_30d", IntegerType(), True),
    StructField("last_sold_date", DateType(), True),
])
store_sales_data = [
    ("SKU001", 80,  date(2025, 7, 10)),
    ("SKU002", 12,  date(2025, 7, 5)),
    ("SKU005", 240, date(2025, 7, 12)),
    ("SKU006", 35,  date(2025, 7, 8)),
    ("SKU007", 18,  date(2025, 7, 6)),  # sold but not in warehouse
    ("SKU008", 5,   date(2025, 7, 1)),  # sold but not in warehouse
    ("SKU009", 22,  date(2025, 7, 11)),
    ("SKU011", 65,  date(2025, 7, 9)),
]
store_sales = spark.createDataFrame(store_sales_data, store_sales_schema)
store_sales.createOrReplaceTempView("store_sales")

suppliers_schema = StructType([
    StructField("supplier_id", IntegerType(), False),
    StructField("supplier_name", StringType(), True),
    StructField("region", StringType(), True),
])
suppliers_data = [
    (1, "VedaFoods",       "MH"),
    (2, "AgroLink",        "GJ"),
    (3, "Nava Distributors","KA"),
    (4, "FreshHarvest",    "MH"),
    (5, "PrimeSupply",     "GJ"),
]
suppliers = spark.createDataFrame(suppliers_data, suppliers_schema)
suppliers.createOrReplaceTempView("suppliers")

supplier_catalog_schema = StructType([
    StructField("supplier_id", IntegerType(), True),
    StructField("sku", StringType(), True),
    StructField("status", StringType(), True),
])
supplier_catalog_data = [
    (1, "SKU001", "active"),
    (1, "SKU002", "active"),
    (1, "SKU012", "discontinued"),
    (2, "SKU003", "active"),
    (2, "SKU004", "active"),
    (2, "SKU005", "active"),
    (3, "SKU006", "active"),
    (3, "SKU009", "active"),
    (4, "SKU010", "active"),
    (4, "SKU011", "active"),
    (4, "SKU013", "active"),
    (5, "SKU014", "discontinued"),
]
supplier_catalog = spark.createDataFrame(supplier_catalog_data, supplier_catalog_schema)
supplier_catalog.createOrReplaceTempView("supplier_catalog")

print(f"warehouse: {warehouse.count()}, store_sales: {store_sales.count()}, suppliers: {suppliers.count()}, supplier_catalog: {supplier_catalog.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Stock reconciliation between the two systems
# MAGIC
# MAGIC Reconcile warehouse vs store_sales. Show every SKU that exists in at least one of the two tables. For each, show warehouse_qty and units_sold_30d (NULL if missing).
# MAGIC
# MAGIC Output columns: `sku`, `warehouse_qty`, `units_sold_30d`, `status`.
# MAGIC
# MAGIC The `status` column should be:
# MAGIC - "warehouse_only" if in warehouse but not in store_sales
# MAGIC - "sales_only" if in store_sales but not in warehouse
# MAGIC - "both" if in both
# MAGIC
# MAGIC Sort by sku. Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Dead-stock identification
# MAGIC
# MAGIC The CFO's question: which SKUs are sitting in the warehouse but have ZERO sales in the last 30 days?
# MAGIC
# MAGIC Output columns: `sku`, `warehouse_qty`, `last_received_date`. Sort by warehouse_qty descending (biggest dead stock first).
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Suppliers with at least one active seller
# MAGIC
# MAGIC The COO wants the suppliers that have at least one SKU which is actively selling (appears in store_sales).
# MAGIC
# MAGIC Output columns: `supplier_id`, `supplier_name`, `region`. Sort by supplier_id.
# MAGIC
# MAGIC Avoid producing duplicate supplier rows when one supplier matches multiple sellers. Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Subquery vs JOIN
# MAGIC
# MAGIC The CFO's follow-up: for the dead stock SKUs (warehouse but no sales), who is the supplier?
# MAGIC
# MAGIC ### Part A
# MAGIC Solve using a **subquery** in the WHERE clause.
# MAGIC
# MAGIC ### Part B
# MAGIC Solve using a **JOIN**.
# MAGIC
# MAGIC Output columns for both: `sku`, `warehouse_qty`, `supplier_name`. Sort by warehouse_qty descending.
# MAGIC
# MAGIC Show both work, then in a comment, note which one is generally preferred in PySpark and why.

# COMMAND ----------

# Your Part A (subquery) solution here

# COMMAND ----------

# Your Part B (join) solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · IN vs EXISTS
# MAGIC
# MAGIC The merchandising head asks: list all warehouse SKUs that are part of an "active" supplier catalog entry.
# MAGIC
# MAGIC ### Part A
# MAGIC Solve using `IN (subquery)`.
# MAGIC
# MAGIC ### Part B
# MAGIC Solve using `EXISTS (correlated subquery)`.
# MAGIC
# MAGIC Output for both: `sku`, `warehouse_qty`. Sort by sku.
# MAGIC
# MAGIC In a comment, note when IN and EXISTS behave differently (specifically around NULL handling).

# COMMAND ----------

# Your Part A (IN) solution here

# COMMAND ----------

# Your Part B (EXISTS) solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## What done looks like
# MAGIC
# MAGIC - Task 1: FULL OUTER JOIN with status classification.
# MAGIC - Task 2: Dead stock identified via LEFT ANTI JOIN.
# MAGIC - Task 3: Suppliers with active sellers via LEFT SEMI JOIN.
# MAGIC - Task 4: Same problem solved two ways (subquery and JOIN) with a note on preference.
# MAGIC - Task 5: IN vs EXISTS demonstrated, with a comment on NULL handling.
