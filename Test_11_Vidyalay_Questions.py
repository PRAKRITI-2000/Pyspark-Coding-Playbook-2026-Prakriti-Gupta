# Databricks notebook source
# MAGIC %md
# MAGIC # Test 11 · Vidyalay Learning · Questions
# MAGIC
# MAGIC **Medium · ~75-90 min · 5 tasks**
# MAGIC
# MAGIC *Bengaluru · K-12 EdTech · Series B · 1,400 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC Vidyalay Learning is a five-year-old K-12 platform serving 1.2 million students across 12 Indian languages. Test scores, attendance, course completions, and engagement metrics feed a learning analytics layer that informs both teacher dashboards and parent reports.
# MAGIC
# MAGIC ### What just happened
# MAGIC Last quarter's parent satisfaction survey flagged that score percentile reports were inconsistent across grade-subject combinations. Some reports used median, some used arithmetic mean of percentiles. The Head of Academics wants standardised percentile reporting using statistically defensible methods before the next academic year starts in six weeks.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The Head of Academics wants per-course top-N students and percentile-based bands. The CTO wants the analytics layer to handle 50 million test attempts without blowing up. The product team wants per-student percentile cards.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the data engineer on the analytics layer. Reinforce the window-function patterns, layer in percentile_cont and percentile_disc, and demonstrate NTILE for student banding.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date

students_schema = StructType([
    StructField("student_id", IntegerType(), False),
    StructField("student_name", StringType(), True),
    StructField("grade", IntegerType(), True),
    StructField("city", StringType(), True),
])
students_data = [
    (1, "Aanya Kapoor",   8, "Bengaluru"),
    (2, "Vihaan Sharma",  8, "Pune"),
    (3, "Ishaan Reddy",   8, "Hyderabad"),
    (4, "Saanvi Iyer",    8, "Chennai"),
    (5, "Arjun Mehta",    8, "Bengaluru"),
    (6, "Ananya Joshi",   9, "Mumbai"),
    (7, "Kabir Bose",     9, "Kolkata"),
    (8, "Diya Pillai",    9, "Bengaluru"),
    (9, "Rohan Singh",    9, "Delhi"),
    (10,"Myra Nair",      9, "Kochi"),
    (11,"Vivaan Patel",   10,"Ahmedabad"),
    (12,"Kavya Rao",      10,"Hyderabad"),
    (13,"Aditya Gupta",   10,"Pune"),
    (14,"Riya Verma",     10,"Bengaluru"),
    (15,"Aarav Khanna",   10,"Delhi"),
]
students = spark.createDataFrame(students_data, students_schema)
students.createOrReplaceTempView("students")

scores_schema = StructType([
    StructField("attempt_id", IntegerType(), False),
    StructField("student_id", IntegerType(), True),
    StructField("course", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("attempt_date", DateType(), True),
])
scores_data = [
    # Math
    (1001, 1, "math", 78, date(2025, 6, 10)),
    (1002, 2, "math", 92, date(2025, 6, 10)),
    (1003, 3, "math", 65, date(2025, 6, 10)),
    (1004, 4, "math", 88, date(2025, 6, 10)),
    (1005, 5, "math", 71, date(2025, 6, 10)),
    (1006, 6, "math", 84, date(2025, 6, 10)),
    (1007, 7, "math", 95, date(2025, 6, 10)),
    (1008, 8, "math", 77, date(2025, 6, 10)),
    (1009, 9, "math", 81, date(2025, 6, 10)),
    (1010,10, "math", 69, date(2025, 6, 10)),
    (1011,11, "math", 90, date(2025, 6, 10)),
    (1012,12, "math", 73, date(2025, 6, 10)),
    (1013,13, "math", 86, date(2025, 6, 10)),
    (1014,14, "math", 79, date(2025, 6, 10)),
    (1015,15, "math", 94, date(2025, 6, 10)),
    # Science
    (2001, 1, "science", 82, date(2025, 6, 15)),
    (2002, 2, "science", 75, date(2025, 6, 15)),
    (2003, 3, "science", 91, date(2025, 6, 15)),
    (2004, 4, "science", 68, date(2025, 6, 15)),
    (2005, 5, "science", 88, date(2025, 6, 15)),
    (2006, 6, "science", 73, date(2025, 6, 15)),
    (2007, 7, "science", 80, date(2025, 6, 15)),
    (2008, 8, "science", 96, date(2025, 6, 15)),
    (2009, 9, "science", 64, date(2025, 6, 15)),
    (2010,10, "science", 85, date(2025, 6, 15)),
    (2011,11, "science", 79, date(2025, 6, 15)),
    (2012,12, "science", 92, date(2025, 6, 15)),
    (2013,13, "science", 70, date(2025, 6, 15)),
    (2014,14, "science", 87, date(2025, 6, 15)),
    (2015,15, "science", 76, date(2025, 6, 15)),
]
scores = spark.createDataFrame(scores_data, scores_schema)
scores.createOrReplaceTempView("scores")

print(f"students: {students.count()}, scores: {scores.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Top 3 students per grade per course
# MAGIC
# MAGIC For each (grade, course) combination, return the top 3 students by score.
# MAGIC
# MAGIC Output columns: `grade`, `course`, `student_name`, `score`, `rank_in_grade_course`. Sort by grade, course, rank.
# MAGIC
# MAGIC Exactly 3 rows per group — no ties stretching the result to 4 or more. Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Percentile thresholds per course
# MAGIC
# MAGIC Compute the 50th, 75th, and 90th percentile scores for each course, using two different SQL percentile functions. One of them returns an actual value from the data; the other interpolates.
# MAGIC
# MAGIC Output columns: `course`, `p50_disc`, `p50_cont`, `p75_disc`, `p75_cont`, `p90_disc`, `p90_cont`. Sort by course.
# MAGIC
# MAGIC In a comment, explain the difference between the two.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Per-student rank within high-performing grades
# MAGIC
# MAGIC The Head of Academics asks: list grades where the AVERAGE math score is above 80, but ALSO show each student's individual rank within their grade so the teacher can see who is below par.
# MAGIC
# MAGIC This blends aggregation (HAVING on avg) with a window (per-grade rank).
# MAGIC
# MAGIC Steps:
# MAGIC 1. Filter to grades where AVG(math score) > 80.
# MAGIC 2. For those grades, compute each student's rank within their grade for math.
# MAGIC
# MAGIC Output columns: `grade`, `student_name`, `score`, `rank_in_grade`. Sort by grade, rank.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Per-student percentile metrics
# MAGIC
# MAGIC For each student in each course, compute two percentile-style metrics in two new columns named `cume_dist` and `percent_rank`:
# MAGIC - `cume_dist`: the cumulative distribution (fraction of students in the course with score <= this student's)
# MAGIC - `percent_rank`: the percent rank (0 for lowest, 1 for highest, scaled across N-1 students)
# MAGIC
# MAGIC Output columns: `course`, `student_name`, `score`, `cume_dist`, `percent_rank`. Sort by course, score descending.
# MAGIC
# MAGIC Solve in SQL first, then PySpark.

# COMMAND ----------

# Your SQL solution here

# COMMAND ----------

# Your PySpark solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Quartile bands per course
# MAGIC
# MAGIC The Head of Academics wants students banded into 4 equal-size buckets per course (quartiles) for reporting:
# MAGIC - Q1 (top quartile by score)
# MAGIC - Q2
# MAGIC - Q3
# MAGIC - Q4 (bottom quartile)
# MAGIC
# MAGIC Output columns: `course`, `student_name`, `score`, `quartile_label`. Sort by course, score descending.
# MAGIC
# MAGIC The quartile_label is the string "Q1" through "Q4", where Q1 holds the top-scoring quartile and Q4 the bottom.
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
# MAGIC - Task 1: Top 3 per (grade, course) via ROW_NUMBER.
# MAGIC - Task 2: percentile_disc vs percentile_cont demonstrated with explanation.
# MAGIC - Task 3: HAVING filter on aggregate combined with per-grade ranking.
# MAGIC - Task 4: cume_dist and percent_rank computed per course.
# MAGIC - Task 5: NTILE(4) producing four equal-size buckets per course.
