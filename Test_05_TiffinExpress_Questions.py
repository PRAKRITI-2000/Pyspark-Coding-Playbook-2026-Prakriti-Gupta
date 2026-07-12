# Databricks notebook source
# MAGIC %md
# MAGIC # Test 05 · TiffinExpress · Questions
# MAGIC
# MAGIC **Easy · ~30-40 min · 5 tasks**
# MAGIC
# MAGIC *Hyderabad · Food Delivery · Series A · 950 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC TiffinExpress is a three-year-old subscription tiffin delivery service operating in Hyderabad, Bengaluru, and Pune. Home-cooked meals delivered to office workers twice a day. Customer base of 180,000 active subscribers. Series A closed in March.
# MAGIC
# MAGIC ### What just happened
# MAGIC Customer service has been drowning in complaints about delivery timing. The complaints reference customers by partial address strings, phone numbers in five different formats, and dietary preferences that nobody can parse cleanly. The data flowing in from the call centre is a mess.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The VP Customer Experience wants a clean customer view she can use to triage complaints. The Head of Operations wants delivery duration buckets so the ops team knows where the bottleneck is. The CEO wants both before the all-hands on Friday.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the data engineer assigned to clean up the customer data and make it queryable. The data is dirty in ways you have not seen before. Get the cleaning right and the rest follows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as F
from datetime import datetime

customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("title", StringType(), True),
    StructField("phone_raw", StringType(), True),
    StructField("full_address", StringType(), True),
    StructField("dietary_tags", StringType(), True),
])
customers_data = [
    (1, "Anjali",  "Rao",     "Ms",  "+91-98450-12345",      "Flat 304, Lotus Apts, Hi-Tech City, Hyderabad - 500081",   "veg,jain,no_onion"),
    (2, "Rohan",   "Pillai",  "Mr",  "9845012346",           "12/A, Greenwood, Koramangala, Bengaluru - 560034",         "non_veg,no_pork"),
    (3, "Suman",   "Kumar",   "Dr",  "+91 9876543210",       "B-501, Pinnacle Tower, Aundh, Pune - 411007",              "veg,vegan"),
    (4, "Meera",   "Nair",    "Mrs", "+91-98765-43211",      "Plot 22, Jubilee Hills, Hyderabad - 500033",               "veg,jain"),
    (5, "Vivek",   "Sharma",  "Mr",  "98765-43212",          "404, Whitefield Heights, Whitefield, Bengaluru - 560066",  "non_veg,no_beef"),
    (6, "Kavya",   "Iyer",    "Ms",  "+91 98765 43213",      "C-12, Magnolia Apts, Banjara Hills, Hyderabad - 500034",   "veg,no_onion,no_garlic"),
    (7, "Arjun",   "Mehra",   "Mr",  "9876543214",           "207, Skyline Residency, Hinjewadi, Pune - 411057",         "non_veg"),
    (8, "Priya",   "Gupta",   "Dr",  "+91-98765-43215",      "Villa 7, Palm Grove, Indiranagar, Bengaluru - 560038",     "veg,jain,no_onion,no_garlic"),
    (9, "Aakash",  "Chopra",  "Mr",  "+91 9876543216",       "11, Vasant Marg, Madhapur, Hyderabad - 500081",            "non_veg,no_pork,no_beef"),
    (10,"Neha",    "Singh",   "Ms",  "98765 43217",          "Flat 9B, Royal Court, Baner, Pune - 411045",               "veg"),
]
customers = spark.createDataFrame(customers_data, customers_schema)
customers.createOrReplaceTempView("customers")

deliveries_schema = StructType([
    StructField("delivery_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("scheduled_at", TimestampType(), True),
    StructField("delivered_at", TimestampType(), True),
])
deliveries_data = [
    (101, 1, datetime(2025, 7, 1, 12, 30), datetime(2025, 7, 1, 12, 45)),
    (102, 2, datetime(2025, 7, 1, 13, 0),  datetime(2025, 7, 1, 13, 22)),
    (103, 3, datetime(2025, 7, 1, 12, 30), datetime(2025, 7, 1, 13, 5)),
    (104, 4, datetime(2025, 7, 1, 13, 0),  datetime(2025, 7, 1, 13, 8)),
    (105, 5, datetime(2025, 7, 1, 12, 30), datetime(2025, 7, 1, 13, 35)),
    (106, 6, datetime(2025, 7, 2, 12, 30), datetime(2025, 7, 2, 12, 42)),
    (107, 7, datetime(2025, 7, 2, 13, 0),  datetime(2025, 7, 2, 13, 50)),
    (108, 8, datetime(2025, 7, 2, 12, 30), datetime(2025, 7, 2, 12, 38)),
    (109, 9, datetime(2025, 7, 3, 13, 0),  datetime(2025, 7, 3, 13, 25)),
    (110,10, datetime(2025, 7, 3, 12, 30), datetime(2025, 7, 3, 13, 15)),
]
deliveries = spark.createDataFrame(deliveries_data, deliveries_schema)
deliveries.createOrReplaceTempView("deliveries")

print(f"customers: {customers.count()}, deliveries: {deliveries.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Extract city and pincode from full_address
# MAGIC
# MAGIC The address strings follow a loose pattern: `"<rest of address>, <city> - <pincode>"`. Examples:
# MAGIC - `"Flat 304, Lotus Apts, Hi-Tech City, Hyderabad - 500081"`
# MAGIC - `"B-501, Pinnacle Tower, Aundh, Pune - 411007"`
# MAGIC
# MAGIC Extract two new columns:
# MAGIC - `city`: the segment just before the " - " separator
# MAGIC - `pincode`: the 6-digit number after the separator
# MAGIC
# MAGIC Output columns: `customer_id`, `full_address`, `city`, `pincode`. Sort by customer_id.
# MAGIC
# MAGIC Use `regexp_extract` (PySpark) or `REGEXP_EXTRACT` (SQL). Solve in both.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Normalise phone numbers
# MAGIC
# MAGIC The phone_raw column has phone numbers in mixed formats:
# MAGIC - `"+91-98450-12345"`
# MAGIC - `"9845012346"`
# MAGIC - `"+91 9876543210"`
# MAGIC - `"98765-43212"`
# MAGIC - `"98765 43217"`
# MAGIC
# MAGIC Produce a single canonical 10-digit phone number per customer: strip the country code, hyphens, spaces, plus signs. Output must be exactly 10 digits.
# MAGIC
# MAGIC Output columns: `customer_id`, `phone_raw`, `phone_normalised`. Sort by customer_id.
# MAGIC
# MAGIC Use `regexp_replace` to strip non-digits, then take the last 10 digits.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Build a single display name from parts
# MAGIC
# MAGIC Build a `display_name` in the format `"<title>. <first_name> <last_name>"`.
# MAGIC
# MAGIC Example: customer 1 → `"Ms. Anjali Rao"`, customer 3 → `"Dr. Suman Kumar"`.
# MAGIC
# MAGIC Use `concat` and `concat_ws` appropriately. Show that `concat_ws` skips NULLs while `concat` propagates them (introduce one NULL last_name for the demo).
# MAGIC
# MAGIC Output columns: `customer_id`, `display_name_concat`, `display_name_concat_ws`. Sort by customer_id.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Delivery duration buckets
# MAGIC
# MAGIC Compute delivery duration in minutes (delivered_at minus scheduled_at). Bucket into:
# MAGIC - "on_time": <= 15 minutes
# MAGIC - "slight_delay": 16 to 30 minutes
# MAGIC - "late": 31 to 45 minutes
# MAGIC - "severe": > 45 minutes
# MAGIC
# MAGIC Output columns: `delivery_id`, `customer_id`, `duration_min`, `bucket`. Sort by duration_min descending.
# MAGIC
# MAGIC Use `unix_timestamp` arithmetic or the `(delivered_at - scheduled_at)` interval. Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Parse dietary_tags into separate boolean flags
# MAGIC
# MAGIC The dietary_tags column is comma-separated, e.g. `"veg,jain,no_onion"`.
# MAGIC
# MAGIC Split it into separate boolean flag columns:
# MAGIC - `is_veg` (True if "veg" tag present, False otherwise)
# MAGIC - `is_jain` (True if "jain" tag present)
# MAGIC - `is_no_onion` (True if "no_onion" tag present)
# MAGIC - `is_no_garlic` (True if "no_garlic" tag present)
# MAGIC
# MAGIC Output columns: `customer_id`, `first_name`, `dietary_tags`, `is_veg`, `is_jain`, `is_no_onion`, `is_no_garlic`. Sort by customer_id.
# MAGIC
# MAGIC Use `split` to break the string into an array, then `array_contains` to check membership.

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
# MAGIC - Task 1: City and pincode extracted from messy address strings.
# MAGIC - Task 2: Phone numbers normalised to 10-digit canonical form.
# MAGIC - Task 3: Display name built two ways, with concat vs concat_ws behaviour shown.
# MAGIC - Task 4: Delivery durations bucketed using CASE WHEN.
# MAGIC - Task 5: Dietary tags split and flagged.
