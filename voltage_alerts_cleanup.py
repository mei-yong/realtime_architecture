# Databricks notebook source
# MAGIC %md
# MAGIC # Voltage Alerts Clean-up
# MAGIC Objective: Batch 'clean-up' for both bronze and gold delta tables for voltage alerts. Includes removal of data older than 3 months, unpacking delta log transactions in bronze & gold layers, and data reconcilation scripts for counts of records received<br>
# MAGIC Author(s): annie.ho@baringa.com <br>
# MAGIC Date Updated: 2022-06-23 <br>

# COMMAND ----------

# Import libraries
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import *
from delta.tables import *
from pyspark import Row
import datetime
import time
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove data older than 3 months

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delta Bronze Layer

# COMMAND ----------

# deltaTable = DeltaTable.forPath(spark, "/mnt/bronze/IIB/voltagealerts/voltage_alerts_raw") 

# deltaTable.vacuum()        # vacuum files not required by versions older than the default retention period of 7 days

# deltaTable.vacuum(168)     # vacuum files not required by versions more than 168 hours old ( 6 days)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM DELTA.`/mnt/bronze/IIB/voltagealerts/voltage_alerts_raw`
# MAGIC WHERE MONTHS_BETWEEN(CURRENT_DATE(),ReceivedDate)>3

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delta Gold Layer (view)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM DELTA.`/mnt/gold/IIB/voltage_alerts`
# MAGIC WHERE MONTHS_BETWEEN(CURRENT_TIMESTAMP(),event_timestamp)>3

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delta Gold Layer (History)

# COMMAND ----------

# TBC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Logging

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bronze ACID transaction log

# COMMAND ----------

# method- getting number of rows written to bronze layer from delta history logs 
bronze_deltaTable = DeltaTable.forPath(spark, "/mnt/bronze/IIB/voltagealerts/voltage_alerts_raw")

bronze_fullHistoryDF = bronze_deltaTable.history()    # get the full history of the table

# lastOperationDF = deltaTable.history(1) # get the last operation


# COMMAND ----------

bronze_logs = bronze_fullHistoryDF.withColumn("num_rows_written", F.col("operationMetrics").getItem("numOutputRows"))\
.withColumn("num_bytes_written", F.col("operationMetrics").getItem("numOutputBytes"))\
.withColumn("timestamp",from_utc_timestamp(col("timestamp"),'Europe/London'))\
        .select("version","timestamp","operation","num_rows_written","num_bytes_written")

# COMMAND ----------

display(bronze_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold ACID transaction log

# COMMAND ----------

# method- getting number of rows written to gold layer from delta history logs 
gold_deltaTable = DeltaTable.forPath(spark, "/mnt/gold/IIB/vw_voltage_alerts")

gold_fullHistoryDF = gold_deltaTable.history()    # get the full history of the table


# COMMAND ----------

gold_logs = gold_fullHistoryDF.withColumn("num_rows_written", F.col("operationMetrics").getItem("numOutputRows"))\
                              .withColumn("num_rows_inserted", F.col("operationMetrics").getItem("numTargetRowsInserted"))\
                              .withColumn("num_rows_updated", F.col("operationMetrics").getItem("numTargetRowsUpdated"))\
                              .withColumn("num_rows_deleted", F.col("operationMetrics").getItem("numTargetRowsDeleted"))\
                              .withColumn("execution_time_seconds", (F.col("operationMetrics").getItem("executionTimeMs"))/1000)\
                              .withColumn("scan_time_seconds", (F.col("operationMetrics").getItem("scanTimeMs"))/1000)\
                              .withColumn("rewrite_time_seconds", (F.col("operationMetrics").getItem("rewriteTimeMs"))/1000)\
                              .withColumn("timestamp",from_utc_timestamp(col("timestamp"),'Europe/London'))\
.select("version","timestamp","operation","num_rows_inserted","num_rows_written","num_rows_updated","num_rows_deleted","execution_time_seconds","scan_time_seconds","rewrite_time_seconds")

# COMMAND ----------

display(gold_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Reconciliation

# COMMAND ----------

# MAGIC %md
# MAGIC ##### data recon bronze hourly by received time
# MAGIC 
# MAGIC Can only group by 'recevied_time' as the 'event_time' has not yet been decoded in the raw data stage

# COMMAND ----------

# Read from bronze delta table
df_raw = (spark
            .read
            .format('delta')
            .option("ignoreDeletes", "true")                                       # option added as data cleaup done by 'ReceivedDate' in bronze layer
            .load("/mnt/bronze/IIB/voltagealerts/voltage_alerts_raw")
            )

# COMMAND ----------

# calculates number of records in voltage_alerts_raw grouped by hour

windowedCounts = df_raw.groupBy(window("ReceivedTime", "1 hour", "1 hour")).count().orderBy("window")\
                .withColumn("received_start_time",F.col("window").getItem("start")).withColumn("received_end_time",F.col("window").getItem("end")).select("received_start_time","received_end_time","count")

# COMMAND ----------

display(windowedCounts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### data recon gold view hourly count by event_time

# COMMAND ----------

# Read from gold delta table
df_gold_view = (spark
            .read
            .format('delta')
            .option("ignoreDeletes", "true")                                       # option added as data cleaup done by 'ReceivedDate' in bronze layer
            .load("/mnt/gold/IIB/vw_voltage_alerts")
            )

# COMMAND ----------

# calculates number of records in vw_voltage_alerts grouped by hour ( event_timestamp)

df_gold_view_windowedCounts = df_gold_view.groupBy(window("event_timestamp", "1 hour", "1 hour")).count().orderBy("window")\
                .withColumn("start_time",F.col("window").getItem("start")).withColumn("end_time",F.col("window").getItem("end")).select("start_time","end_time","count")

# COMMAND ----------

display(df_gold_view_windowedCounts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### data recon gold (non-deduplicated) hourly count by received_time 
# MAGIC 
# MAGIC This can be used to cross - check numbers against bronze incoming data

# COMMAND ----------

# Read from gold delta table
df_gold = (spark
            .read
            .format('delta')
            .option("ignoreDeletes", "true")                                       # option added as data cleaup done by 'ReceivedDate' in bronze layer
            .load("/mnt/gold/IIB/voltage_alerts")
            )

# COMMAND ----------

# calculates number of records in vw_voltage_alerts grouped by hour ( received_timestamp)

df_gold_windowedCounts = df_gold.groupBy(window("received_timestamp", "1 hour", "1 hour")).count().orderBy("window")\
                .withColumn("start_time",F.col("window").getItem("start")).withColumn("end_time",F.col("window").getItem("end")).select("start_time","end_time","count")

# COMMAND ----------

display(df_gold_windowedCounts)

# COMMAND ----------

# calculates number of records in vw_voltage_alerts grouped by hour ( event_timestamp)

df_gold_windowedCounts = df_gold.groupBy(window("event_timestamp", "1 hour", "1 hour")).count().orderBy("window")\
                .withColumn("event_start_time",F.col("window").getItem("start")).withColumn("event_end_time",F.col("window").getItem("end")).select("event_start_time","event_end_time","count")

# COMMAND ----------

display(df_gold_windowedCounts)
