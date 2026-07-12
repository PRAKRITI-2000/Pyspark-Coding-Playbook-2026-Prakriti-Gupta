# Databricks notebook source
# MAGIC %md
# MAGIC # Test 13 · MediCore Health · Questions
# MAGIC
# MAGIC **Hard · ~120 min · 5 tasks**
# MAGIC
# MAGIC *Hyderabad · Hospital Network · Private · 38,000 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC MediCore Health runs a 22-hospital network across South India. Patient records, encounter data, billing, and diagnosis codes flow from each hospital's EHR into a central data lake. The schemas drift constantly: each EHR upgrade adds optional fields, some hospitals send nested JSON for diagnostic findings, some flatten everything.
# MAGIC
# MAGIC ### What just happened
# MAGIC The Chief Medical Officer asked for a "single patient view" linking diagnoses to billing to providers. The first attempt at building it failed because the diagnosis data is deeply nested JSON, two hospitals send schemas with extra optional fields, and the data quality is uneven. The clinical analytics team has been hand-coding workarounds for six months.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The CMO wants the integrated view by end of quarter. The compliance lead wants a data quality framework attached so audits stop being painful. The CTO wants the schema drift handled at the platform layer, not in 14 different downstream queries.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the senior data engineer on the platform team. Define the nested schemas, flatten the JSON cleanly, handle drift, and build the quality framework.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField, IntegerType, StringType, DoubleType,
                                DateType, ArrayType, MapType, LongType)
from pyspark.sql import functions as F
from datetime import date

# Patients
patients_schema = StructType([
    StructField("patient_id", IntegerType(), False),
    StructField("patient_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("city", StringType(), True),
])
patients_data = [
    (1001, "Anita Rao",     date(1980, 5, 12), "Hyderabad"),
    (1002, "Bhanu Reddy",   date(1965, 3, 8),  "Bengaluru"),
    (1003, "Chetan Iyer",   date(1972, 11, 22),"Chennai"),
    (1004, "Divya Pillai",  date(1990, 7, 4),  "Kochi"),
    (1005, "Eshan Bose",    date(1988, 1, 30), "Hyderabad"),
    (1006, "Faiz Khan",     None,              "Hyderabad"),  # null DOB
]
patients = spark.createDataFrame(patients_data, patients_schema)
patients.createOrReplaceTempView("patients")

# Encounters
encounters_schema = StructType([
    StructField("encounter_id", IntegerType(), False),
    StructField("patient_id", IntegerType(), True),
    StructField("provider_id", IntegerType(), True),
    StructField("encounter_date", DateType(), True),
    StructField("encounter_type", StringType(), True),
])
encounters_data = [
    (10001, 1001, 501, date(2025, 7, 1), "outpatient"),
    (10002, 1002, 502, date(2025, 7, 1), "inpatient"),
    (10003, 1003, 501, date(2025, 7, 2), "outpatient"),
    (10004, 1001, 503, date(2025, 7, 5), "outpatient"),
    (10005, 1004, 502, date(2025, 7, 7), "emergency"),
    (10006, 1005, 504, date(2025, 7, 9), "outpatient"),
    (10007, 1001, 501, date(2025, 7, 12),"outpatient"),
    (10008, 1006, 504, date(2025, 7, 14),"outpatient"),
    (10009, 9999, 501, date(2025, 7, 15),"outpatient"),  # orphan: patient_id 9999 doesn't exist
]
encounters = spark.createDataFrame(encounters_data, encounters_schema)
encounters.createOrReplaceTempView("encounters")

# Providers
providers_schema = StructType([
    StructField("provider_id", IntegerType(), False),
    StructField("provider_name", StringType(), True),
    StructField("specialty", StringType(), True),
])
providers_data = [
    (501, "Dr Suresh Iyer",   "cardiology"),
    (502, "Dr Anjali Sharma", "general_medicine"),
    (503, "Dr Rohit Kumar",   "orthopedics"),
    (504, "Dr Meera Joshi",   "general_medicine"),
]
providers = spark.createDataFrame(providers_data, providers_schema)
providers.createOrReplaceTempView("providers")

# Billing
billing_schema = StructType([
    StructField("bill_id", IntegerType(), False),
    StructField("encounter_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("paid", DoubleType(), True),
    StructField("billing_date", DateType(), True),
])
billing_data = [
    (20001, 10001, 5000.0, 5000.0, date(2025, 7, 1)),
    (20002, 10002, 75000.0, 50000.0, date(2025, 7, 2)),
    (20003, 10003, 3000.0, 3000.0, date(2025, 7, 2)),
    (20004, 10004, 4500.0, 4500.0, date(2025, 7, 5)),
    (20005, 10005, 25000.0, 25000.0, date(2025, 7, 7)),
    (20006, 10006, 3500.0, 0.0,   date(2025, 7, 9)),
    (20007, 10007, 5500.0, 5500.0, date(2025, 7, 12)),
    (20008, 10008, 2800.0, 2800.0, date(2025, 7, 14)),
    # Note: no billing for encounter 10009
]
billing = spark.createDataFrame(billing_data, billing_schema)
billing.createOrReplaceTempView("billing")

# Diagnosis with nested JSON-like findings
diagnosis_json_schema = StructType([
    StructField("encounter_id", IntegerType(), False),
    StructField("findings_json", StringType(), True),  # JSON string
])
diagnosis_json_data = [
    (10001, '{"primary":"E11.9","secondary":["I10","E78.5"],"severity":{"level":"moderate","scale":"GRADE"}}'),
    (10002, '{"primary":"I21.4","secondary":["I10","E78.5","Z95.1"],"severity":{"level":"severe","scale":"GRADE"}}'),
    (10003, '{"primary":"M54.5","secondary":[],"severity":{"level":"mild","scale":"GRADE"}}'),
    (10004, '{"primary":"J45.9","secondary":["J30.9"],"severity":{"level":"moderate","scale":"GRADE"}}'),
    (10005, '{"primary":"S52.5","secondary":["T14.1"],"severity":{"level":"severe","scale":"AAST"}}'),
    (10006, '{"primary":"E11.9","secondary":["I10"],"severity":{"level":"moderate","scale":"GRADE"}}'),
    (10007, '{"primary":"E11.9","secondary":["I10","N18.3"],"severity":{"level":"moderate","scale":"GRADE"}}'),
    (10008, '{"primary":"R51","secondary":[],"severity":{"level":"mild","scale":"GRADE"}}'),
]
diagnosis_json = spark.createDataFrame(diagnosis_json_data, diagnosis_json_schema)
diagnosis_json.createOrReplaceTempView("diagnosis_json")

print(f"patients: {patients.count()}, encounters: {encounters.count()}, providers: {providers.count()}, billing: {billing.count()}, diagnosis_json: {diagnosis_json.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Define the patient profile schema
# MAGIC
# MAGIC Define a `patient_profile_schema` that captures:
# MAGIC - `patient_id` (int)
# MAGIC - `name` (string)
# MAGIC - `contacts`: an ArrayType of StructType with fields `type` (string, e.g. "mobile", "email") and `value` (string)
# MAGIC - `allergies`: an ArrayType of string
# MAGIC - `vitals_history`: a MapType from String (date as string) to StructType with `weight_kg` (double), `bp_systolic` (int)
# MAGIC
# MAGIC Create a small DataFrame with two patients using this schema. Print the schema.

# COMMAND ----------

# Your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Parse and flatten the diagnosis findings
# MAGIC
# MAGIC From `diagnosis_json`, parse the `findings_json` column into structured fields with an explicit schema, then:
# MAGIC - Surface `primary` diagnosis code as a top-level column.
# MAGIC - Surface `severity_level` and `severity_scale` from the nested severity struct.
# MAGIC - Turn the `secondary` array into one row per secondary diagnosis.
# MAGIC
# MAGIC Output columns: `encounter_id`, `primary_dx`, `severity_level`, `severity_scale`, `secondary_dx`. Sort by encounter_id, secondary_dx.
# MAGIC
# MAGIC Encounters with empty secondary arrays must still appear in the output, once, with NULL secondary_dx.

# COMMAND ----------

# Your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Absorb the new hospital's extended schema
# MAGIC
# MAGIC Hospital 14 just upgraded its EHR. Its encounters file now includes two extra fields: `triage_score` (int) and `referral_source` (string). The other hospitals still send the old schema.
# MAGIC
# MAGIC ### Part A
# MAGIC Create an "old hospital" DataFrame with the original schema (5 columns), and a "new hospital" DataFrame with the extended schema (7 columns).
# MAGIC
# MAGIC ### Part B
# MAGIC Demonstrate two approaches for absorbing the new optional columns: one that merges schemas at read time (without modifying the source tables), and a commented example of the Delta write-time option that achieves the same result on disk.
# MAGIC
# MAGIC ### Part C
# MAGIC Show the combined result. Old-hospital rows should have NULL for the new fields; new-hospital rows should have their values.

# COMMAND ----------

# Your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Data quality framework
# MAGIC
# MAGIC Build a small data quality framework that returns a single row PER CHECK with columns:
# MAGIC - `check_name` (string)
# MAGIC - `table_name` (string)
# MAGIC - `metric_value` (double or count)
# MAGIC - `status` ("pass" or "fail" based on a threshold)
# MAGIC
# MAGIC Implement at least 4 checks across the tables:
# MAGIC 1. Null percentage of `dob` in patients (fail if > 10%)
# MAGIC 2. Duplicate count on `patient_id` in patients (fail if > 0)
# MAGIC 3. Referential integrity: count of encounters whose patient_id has no matching patients row (fail if > 0)
# MAGIC 4. Coverage: percentage of encounters that have a matching billing row (fail if < 95%)
# MAGIC
# MAGIC Combine all four results into a single DataFrame using `unionByName` and show it.

# COMMAND ----------

# Your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · 5-way join · The Single Patient View
# MAGIC
# MAGIC Build the CMO's single patient view by joining patients, encounters, providers, billing, and the parsed diagnosis (primary only, no secondary explosion).
# MAGIC
# MAGIC Output columns:
# MAGIC - `patient_id`, `patient_name`, `city`
# MAGIC - `encounter_id`, `encounter_date`, `encounter_type`
# MAGIC - `provider_name`, `specialty`
# MAGIC - `primary_dx`, `severity_level`
# MAGIC - `billed_amount`, `paid_amount`, `outstanding` (billed - paid)
# MAGIC
# MAGIC Use INNER JOINs where the relationship is mandatory and LEFT JOINs where it is optional. Document each join's reasoning in comments.
# MAGIC
# MAGIC Sort by patient_id, encounter_date.

# COMMAND ----------

# Your code here

# COMMAND ----------

# MAGIC %md
# MAGIC ### Topics this test covers
# MAGIC Nested schemas, JSON flattening, schema-drift handling, data quality, and multi-table integration. If you're stuck, the relevant material is from weeks 7-10 of the cohort.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## What done looks like
# MAGIC
# MAGIC - Task 1: Nested StructType with ArrayType, MapType demonstrated.
# MAGIC - Task 2: JSON parsed, severity flattened, secondary diagnoses exploded with explode_outer.
# MAGIC - Task 3: Schema drift handled via unionByName + Delta mergeSchema reference.
# MAGIC - Task 4: 4-check data quality framework with status column.
# MAGIC - Task 5: 5-way join producing the single patient view with proper join-type reasoning.
