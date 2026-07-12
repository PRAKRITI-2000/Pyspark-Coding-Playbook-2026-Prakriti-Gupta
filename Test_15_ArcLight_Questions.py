# Databricks notebook source
# MAGIC %md
# MAGIC # Test 15 · ArcLight Capital · Questions
# MAGIC
# MAGIC **Hard · ~120 min · 6 tasks**
# MAGIC
# MAGIC *Mumbai · Proprietary Trading · Privately held · 280 employees*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The setup
# MAGIC ArcLight Capital is a Mumbai-based proprietary trading firm running equities, F&O, and currency strategies across NSE, BSE, and global venues. The platform processes 2 million trades a day, maintains positions and PnL in near real time, and produces SEBI-required T+1 reports by 7am IST.
# MAGIC
# MAGIC ### What just happened
# MAGIC Last quarter, the positions query that drives the morning risk report started taking 22 minutes to complete. The SLA is 4 minutes. Two contributing factors: the positions table has grown to 400GB on Delta, and the trade reconciliation query joins across four tables without proper partitioning or Z-ordering. Compliance got an extension on the SEBI submission deadline once. They will not get a second.
# MAGIC
# MAGIC ### Who's asking
# MAGIC The Chief Risk Officer needs T+1 reports inside the 4-minute SLA. The Head of Trading wants intraday PnL queries to feel instant. The data platform lead wants the storage layout decisions documented for new tables.
# MAGIC
# MAGIC ### Your role
# MAGIC You are the senior data engineer on the trading analytics team. Build the reconciliation logic with complex aggregations, lay out the storage strategy with partitioning + Z-ORDER, schedule OPTIMIZE and VACUUM, and produce time-series snapshots.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup · Sample data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType, LongType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, date

# Instruments dimension (small)
instruments_schema = StructType([
    StructField("instrument_id", IntegerType(), False),
    StructField("symbol", StringType(), True),
    StructField("asset_class", StringType(), True),
    StructField("exchange", StringType(), True),
])
instruments_data = [
    (1, "RELIANCE",    "equity",   "NSE"),
    (2, "HDFCBANK",    "equity",   "NSE"),
    (3, "TCS",         "equity",   "NSE"),
    (4, "INFY",        "equity",   "NSE"),
    (5, "BANKNIFTY-F", "futures",  "NSE"),
    (6, "NIFTY-F",     "futures",  "NSE"),
    (7, "USDINR-F",    "currency", "NSE"),
]
instruments = spark.createDataFrame(instruments_data, instruments_schema)
instruments.createOrReplaceTempView("instruments")

# Trades table
trades_schema = StructType([
    StructField("trade_id", LongType(), False),
    StructField("trade_date", DateType(), True),
    StructField("trade_ts", TimestampType(), True),
    StructField("strategy_id", IntegerType(), True),
    StructField("instrument_id", IntegerType(), True),
    StructField("side", StringType(), True),       # buy / sell
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
])
trades_data = [
    # July 1
    (90001, date(2025,7,1), datetime(2025,7,1,9,30), 1, 1, "buy",  500,  2800.0),
    (90002, date(2025,7,1), datetime(2025,7,1,9,45), 1, 1, "buy",  300,  2810.0),
    (90003, date(2025,7,1), datetime(2025,7,1,10,15),1, 2, "buy",  200,  1650.0),
    (90004, date(2025,7,1), datetime(2025,7,1,11,0), 1, 1, "sell", 400,  2830.0),
    (90005, date(2025,7,1), datetime(2025,7,1,13,30),2, 3, "buy",  100,  3920.0),
    (90006, date(2025,7,1), datetime(2025,7,1,14,0), 2, 5, "buy",  50,   52400.0),
    # July 2
    (90007, date(2025,7,2), datetime(2025,7,2,9,30), 1, 1, "buy",  200,  2825.0),
    (90008, date(2025,7,2), datetime(2025,7,2,10,0), 1, 4, "buy",  500,  1480.0),
    (90009, date(2025,7,2), datetime(2025,7,2,11,30),2, 5, "sell", 25,   52600.0),
    (90010, date(2025,7,2), datetime(2025,7,2,14,0), 1, 2, "sell", 100,  1660.0),
    # July 3
    (90011, date(2025,7,3), datetime(2025,7,3,9,30), 1, 1, "sell", 300,  2840.0),
    (90012, date(2025,7,3), datetime(2025,7,3,10,0), 3, 6, "buy",  100,  23500.0),
    (90013, date(2025,7,3), datetime(2025,7,3,13,0), 3, 7, "buy",  10,   83.50),
    (90014, date(2025,7,3), datetime(2025,7,3,14,30),2, 3, "sell", 50,   3950.0),
]
trades = spark.createDataFrame(trades_data, trades_schema)
trades.createOrReplaceTempView("trades")

# End-of-day prices for mark-to-market
eod_prices_schema = StructType([
    StructField("price_date", DateType(), True),
    StructField("instrument_id", IntegerType(), True),
    StructField("close_price", DoubleType(), True),
])
eod_prices_data = [
    (date(2025,7,1), 1, 2825.0),  (date(2025,7,1), 2, 1655.0),
    (date(2025,7,1), 3, 3930.0),  (date(2025,7,1), 5, 52450.0),
    (date(2025,7,2), 1, 2835.0),  (date(2025,7,2), 2, 1665.0),
    (date(2025,7,2), 3, 3945.0),  (date(2025,7,2), 4, 1490.0),
    (date(2025,7,2), 5, 52550.0),
    (date(2025,7,3), 1, 2845.0),  (date(2025,7,3), 2, 1670.0),
    (date(2025,7,3), 3, 3955.0),  (date(2025,7,3), 4, 1495.0),
    (date(2025,7,3), 5, 52600.0), (date(2025,7,3), 6, 23550.0),
    (date(2025,7,3), 7, 83.60),
]
eod_prices = spark.createDataFrame(eod_prices_data, eod_prices_schema)
eod_prices.createOrReplaceTempView("eod_prices")

# Strategies dimension
strategies_schema = StructType([
    StructField("strategy_id", IntegerType(), False),
    StructField("strategy_name", StringType(), True),
    StructField("strategy_lead", StringType(), True),
])
strategies_data = [
    (1, "MomentumEquity",  "Karthik Subramanian"),
    (2, "MeanReversion",   "Aanya Kapoor"),
    (3, "MacroFutures",    "Rohit Verma"),
]
strategies = spark.createDataFrame(strategies_data, strategies_schema)
strategies.createOrReplaceTempView("strategies")

print(f"instruments: {instruments.count()}, trades: {trades.count()}, eod_prices: {eod_prices.count()}, strategies: {strategies.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Enriched trade view with mark-to-market
# MAGIC
# MAGIC Build a per-trade enriched view joining trades + instruments + strategies + eod_prices (closing price for the trade_date).
# MAGIC
# MAGIC Output columns: `trade_id`, `trade_date`, `strategy_name`, `symbol`, `asset_class`, `side`, `quantity`, `price`, `close_price`, `notional_value` (quantity × price), `gross_mtm` (quantity × (close_price - price) for buys; quantity × (price - close_price) for sells).
# MAGIC
# MAGIC INNER JOIN where the relationship is mandatory, LEFT JOIN where it is optional (a trade without an EOD price is possible if EOD prices haven't been loaded yet).
# MAGIC
# MAGIC Sort by trade_date, trade_id.

# COMMAND ----------

# Your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 · Position snapshots (end-of-day per instrument per strategy)
# MAGIC
# MAGIC For each (trade_date, strategy_id, instrument_id) combination, compute the position as: SUM(quantity for buys) - SUM(quantity for sells).
# MAGIC
# MAGIC Then compute the CUMULATIVE position as of each trade_date (running total per (strategy_id, instrument_id) across days).
# MAGIC
# MAGIC Output columns: `trade_date`, `strategy_name`, `symbol`, `daily_position_change`, `cumulative_position`. Sort by strategy_name, symbol, trade_date.

# COMMAND ----------

# Your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 · Storage layout for the trades table
# MAGIC
# MAGIC ### Part A
# MAGIC Write the Delta CREATE TABLE for `trades_delta` partitioned by `trade_date`. Show the syntax.
# MAGIC
# MAGIC ### Part B
# MAGIC Show an example query that exploits partition pruning: filtering on `trade_date = DATE'2025-07-02'`. In a markdown cell, explain how to verify pruning is happening (look at the physical plan for PartitionFilters).
# MAGIC
# MAGIC ### Part C
# MAGIC Write a CREATE TABLE for `instrument_metadata_delta` and explain WHY you would NOT partition this table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Part A: trades_delta CREATE with PARTITIONED BY (trade_date)
# MAGIC
# MAGIC -- Part B: partition pruning query example
# MAGIC
# MAGIC -- Part C: instrument_metadata_delta CREATE - why no partition

# COMMAND ----------

# MAGIC %md
# MAGIC **Your explanation of pruning verification and the no-partition reasoning here.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Tuning fast lookups by instrument inside a partition
# MAGIC
# MAGIC The trades_delta table is partitioned by trade_date. Inside each daily partition, queries frequently filter on `instrument_id`. With 12,000+ instruments, partitioning on instrument_id alongside date would create too many partitions. Z-ORDER is the right tool.
# MAGIC
# MAGIC ### Part A
# MAGIC Write the `OPTIMIZE trades_delta ZORDER BY (instrument_id)` statement.
# MAGIC
# MAGIC ### Part B
# MAGIC In a markdown cell, explain:
# MAGIC - When Z-ORDER should be run (after batch writes, on a maintenance cadence)
# MAGIC - Why Z-ORDER on the PARTITION column is meaningless
# MAGIC - The trade-off: Z-ORDER rewrites files, costing IO and time
# MAGIC
# MAGIC ### Part C
# MAGIC Write the alternative `OPTIMIZE` (without ZORDER) for plain file compaction. Explain when each is appropriate.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Part A
# MAGIC
# MAGIC -- Part C

# COMMAND ----------

# MAGIC %md
# MAGIC **Your Part B explanation here.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 · Daily table maintenance
# MAGIC
# MAGIC ### Part A
# MAGIC Write a `VACUUM` statement with the default retention (168 hours = 7 days) and one with `RETAIN 720 HOURS` (30 days).
# MAGIC
# MAGIC ### Part B
# MAGIC Explain why you should NOT set VACUUM retention to 0 hours (or anything under the default), and how to override the check if you really mean it (`spark.databricks.delta.retentionDurationCheck.enabled = false`).
# MAGIC
# MAGIC ### Part C
# MAGIC Show the typical daily-maintenance sequence: OPTIMIZE → ZORDER BY → VACUUM.
# MAGIC
# MAGIC ### Part D
# MAGIC In a markdown cell, explain the relationship between VACUUM retention and time travel (you can travel back as far as the oldest non-vacuumed file).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Part A: VACUUM examples
# MAGIC
# MAGIC -- Part C: daily maintenance sequence

# COMMAND ----------

# MAGIC %md
# MAGIC **Parts B and D explanations here.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 · As-of position query (point-in-time)
# MAGIC
# MAGIC The Chief Risk Officer asks: "What was each strategy's position in RELIANCE as of the close of 2025-07-02?"
# MAGIC
# MAGIC Build a function that takes an as-of date and an instrument symbol, and returns the cumulative position per strategy as of that date (inclusive of trades from that date).
# MAGIC
# MAGIC Output columns: `strategy_name`, `symbol`, `as_of_date`, `position`. Sort by strategy_name.
# MAGIC
# MAGIC Demonstrate the function for `('RELIANCE', date(2025, 7, 2))` and `('RELIANCE', date(2025, 7, 3))`.

# COMMAND ----------

# Your function and demonstrations here

# COMMAND ----------

# MAGIC %md
# MAGIC ### Topics this test covers
# MAGIC Multi-table joins, point-in-time queries, and Delta storage tuning (partitioning, intra-partition clustering, table maintenance). If you're stuck, the relevant material is from weeks 9-11 of the cohort.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## What done looks like
# MAGIC
# MAGIC - Task 1: 4-table enriched trade view with INNER/LEFT discipline and MTM calculation.
# MAGIC - Task 2: Daily and cumulative positions per (strategy, instrument).
# MAGIC - Task 3: PARTITIONED BY syntax + partition pruning explanation + no-partition reasoning.
# MAGIC - Task 4: Z-ORDER usage with cost/benefit trade-offs.
# MAGIC - Task 5: VACUUM + OPTIMIZE daily maintenance pattern.
# MAGIC - Task 6: As-of position query producing point-in-time snapshots.
