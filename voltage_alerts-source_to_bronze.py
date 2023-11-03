# Databricks notebook source
# MAGIC %md
# MAGIC # Voltage Alerts (Ingestion)
# MAGIC Objective: Ingests the events from Azure Event Hub and saves the events in bronze delta tables <br>
# MAGIC Author(s): annie.ho@baringa.com <br>
# MAGIC Date Updated: 2022-05-20 <br>
# MAGIC Assumptions:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import and Prep Libraries, User Defined Functions, and Parameters

# COMMAND ----------

# %pip install azure-eventhub


# COMMAND ----------

# Import libraries
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import datetime
import time
import json



# COMMAND ----------

# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")


# COMMAND ----------

# Need to set legacy time parser as Spark 3.0: Fail to recognize 'dd/MM/YYYY' pattern in the DateTimeFormatter
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Establish connection with Event Hub

# COMMAND ----------

# dbutils.secrets.listScopes()
# Read Event Hub connection string from Azure Key vault - Created 'kvscope' from databricks secret scope section following https://adb-5404479024351902.2.azuredatabricks.net#secrets/createScope

# Created "ukpn-edm-eventhub-voltagealerts-connectionstring" in EDM key vault with connection string

# Retrieve secret
connectionstring = dbutils.secrets.get("keyvault-managed","ukpn-edm-eventhub-voltagealerts-connectionstring")

ehConf = {}
ehConf["eventhubs.connectionstring"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionstring)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Specify output locations

# COMMAND ----------

# Setup storage locations for paths

# Location of output data
bronze_path = "/mnt/bronze/IIB/voltagealerts/"

# Location of checkpoint
bronze_checkpoint_path = "/mnt/bronze/checkpoint/voltagealerts/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Messages from Event Hub

# COMMAND ----------

# Event Hubs Configuration - offset and seqNo is only specified for testing purposes

# Create the starting position Dictionary
#startingEventPosition = {
#"offset": -1,
#"seqNo": -1,
#"enqueuedTime": None,
#"isInclusive": True
#}


# Encrypt ehConf connectionString property
#ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

#spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)



# COMMAND ----------

# Read stream directly from Event Hub

df_stream = (spark.readStream
             .format("eventhubs")                                                              # Read from Event Hubs directly
             .options(**ehConf)                                                                # Use the Event-Hub-enabled connect string
             .load()                                                                           # Load the data
             .withColumn("Offset", col("Offset").cast(LongType()))\
             .withColumn("ReceivedTime", from_utc_timestamp(col("enqueuedTime").cast(TimestampType()),'Europe/London'))            # Time that Eventhub received the message - converted to BST
             .withColumn("ReceivedDate", date_format(col("enqueuedTime").cast(TimestampType()),"YYYY-MM-dd"))       # column created for partitioning
             .withColumn("ReceivedYear", date_format(col("enqueuedTime").cast(TimestampType()),"YYYY"))       # column created for partitioning
             .withColumn("ReceivedMonth", date_format(col("enqueuedTime").cast(TimestampType()),"MM"))              # column created for partitioning
             .withColumn("ReceivedDay", date_format(col("enqueuedTime").cast(TimestampType()),"dd"))                # column created for partitioning
             .withColumn("Body", col("body").cast(StringType()))                               # Extract "body" from the payload and convert from binary format to string
             .select ("Offset","ReceivedTime","ReceivedDate","ReceivedYear","ReceivedMonth","ReceivedDay","Body")

)


# COMMAND ----------

# display(df_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output Data

# COMMAND ----------

# Write stream dataframe to bronze delta location 

# Make sure that our working directory exists
# dbutils.fs.mkdirs(bronze_path)  

write_to_delta = ((df_stream)
                  .writeStream.format('delta')  
                  .option("checkpointLocation", bronze_checkpoint_path  + "voltage_alerts_raw")     # Specify the location of checkpoint files & W-A logs
                  .outputMode("append")                                                             # add only new records to output sink
                  .partitionBy("ReceivedYear","ReceivedMonth", "ReceivedDay")                                                      # partition by received date as event timestamp has not been parsed yet
                  .start(bronze_path + "voltage_alerts_raw")
)



# Create the external tables once data starts to stream in
while True:
    try:
        spark.sql(f'CREATE TABLE IF NOT EXISTS voltage_alerts_raw USING DELTA LOCATION "{bronze_path + "voltage_alerts_raw"}"')
        break
    except:
        pass

# COMMAND ----------

# write_to_delta.lastProgress

# COMMAND ----------

#%sql
#SELECT count(*)
#FROM DELTA.`/mnt/bronze/IIB/voltagealerts/voltage_alerts_raw`

# COMMAND ----------

#%sql
#SELECT *
#FROM DELTA.`/mnt/bronze/IIB/voltagealerts/voltage_alerts_raw`

# COMMAND ----------

#%sql
#SELECT *
#FROM DELTA.`/mnt/gold/IIB/vw_voltage_alerts` 
#WHERE event_date>"03/07/2022"
#ORDER BY received_timestamp DESC

# COMMAND ----------

#%sql
#SELECT *
#FROM DELTA.`/mnt/bronze/IIB/voltagealerts/voltage_alerts_raw`
#WHERE ReceivedTime>"2022-07-01T14:00:00.000+0000"
#ORDER BY ReceivedTime DESC
