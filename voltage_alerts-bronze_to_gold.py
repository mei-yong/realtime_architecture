# Databricks notebook source
# MAGIC %md
# MAGIC # Voltage Alerts (Processing)
# MAGIC Objective: Ingests streaming data from delta bronze tables (raw), parses, processes the events with relevant transformations and upserts the Voltage Alerts into the Delta Gold (Enriched) Table<br>
# MAGIC Author(s): annie.ho@baringa.com <br>
# MAGIC Date Updated: 2022-05-20 <br>
# MAGIC 
# MAGIC Here we read a stream of data from `bronze_path` and write another stream to `gold_path`: `vw_voltage_alerts` contains the final enriched view for power BI to connect to and `voltage_alerts`contains data before enrichement & deduplication for analysis
# MAGIC 
# MAGIC The data consists of a lookup between the voltage alerts streaming data alert code, to the Alert code lookup table and then a join between the voltage alerts streaming mpans to the Customer Connections mpans
# MAGIC 
# MAGIC Performing this join allows us to reduce the total number of rows in our table to enable faster refresh times in Power BI

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
# MAGIC ### Import and Prep Libraries, User Defined Functions, and Parameters

# COMMAND ----------

# Location of output data
gold_path = "/mnt/gold/IIB/"
# Location of checkpoint
gold_checkpoint_path = "/mnt/gold/checkpoint/"

# Location of customer connectivitiy delta table
gold_customer_connectivity_path = "/mnt/gold/MSBI/customerconnectivity"

# Location of alert codes lookup 
bronze_alert_codes_lookup = "/mnt/bronze/referencedatasets/SMA.alertcodes_lookup*.csv"

# COMMAND ----------

# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Data & Supporting Info

# COMMAND ----------

# Read a stream from bronze delta table
df_bronze = (spark
            .readStream
            .format('delta')
            .option("ignoreDeletes", "true")                                       # option added as data cleaup done by 'ReceivedDate' in bronze layer
            .load("/mnt/bronze/IIB/voltagealerts/voltage_alerts_raw")
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Parse & Transform the Data

# COMMAND ----------

# Define absolute xpaths + renamed column for all the required attributes

# xpath_guid= "xpath(Body, '/Envelope/Body/MeterReadsReplyMessage/payload/MeterReading/Meter/mRID/text()') guid"
# xpath_mpan = "xpath(Body, '/Envelope/Body/MeterReadsReplyMessage/payload/MeterReading/ServiceDeliveryPoint/mRID/text()') mpan"
# xpath_event_timestamp = "xpath(Body, '/Envelope/Body/MeterReadsReplyMessage/payload/MeterReading/Event/timestamp/text()') event_timestamp"
# xpath_alert_code = "xpath(Body, '/Envelope/Body/MeterReadsReplyMessage/payload/MeterReading/Event/argument[6]/value/text()') alert_code" 
# xpath_end_time = "xpath(Body, '/Envelope/Body/MeterReadsReplyMessage/payload/endTime/text()') end_time"


# COMMAND ----------

# 'Body'column contains a complex object that requires parsing using xpaths
# Define relative xpaths + renamed column for all the required attributes

xpath_guid= "xpath(Body, '//Meter/mRID/text()') guid"                                    # Meter mRID
xpath_mpan = "xpath(Body, '//ServiceDeliveryPoint/mRID/text()') mpan"                    # Service Delivery Point mRID
xpath_event_timestamp = "xpath(Body, '//Event/timestamp/text()') event_timestamp"        # Event Timestamp
xpath_alert_code = "xpath(Body, '//Event/argument[6]/value/text()') alert_code"          # Event Alert Code
xpath_end_time = "xpath(Body, '//payload/endTime/text()') end_time"                      # Alert end time

# COMMAND ----------

# Parse xml and flatten the schema
df_flattened = df_bronze.selectExpr(
    "ReceivedTime as received_timestamp",
    xpath_guid,
    xpath_mpan,
    xpath_event_timestamp,
    xpath_alert_code,
    xpath_end_time
).selectExpr(
    "explode(arrays_zip(guid, mpan,event_timestamp, alert_code, end_time)) Body", "received_timestamp"
).select('Body.*','received_timestamp')

# COMMAND ----------

# Need to set legacy time parser as Spark 3.0: Fail to recognize 'dd/MM/YYYY' pattern in the DateTimeFormatter
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# Inititialize format of timestamp
partition_date_format =  "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

# Adding new 'event_date' and 'event_hour' columns
df_flattened = df_flattened.withColumn("event_timestamp", to_timestamp(col("event_timestamp"), partition_date_format))\
                   .withColumn("event_date", to_date(date_format(col("event_timestamp"), "yyyy-MM-dd"),"yyyy-MM-dd"))\
                   .withColumn("event_year", date_format(col("event_timestamp"), "yyyy"))\
                   .withColumn("event_month", date_format(col("event_timestamp"), "MM"))\
                   .withColumn("event_day", date_format(col("event_timestamp"), "dd"))\
                   .withColumn("event_hour", date_format(col("event_timestamp"), "HH"))\
                   .withColumn("end_time", to_timestamp(col("end_time"), partition_date_format))

# COMMAND ----------

# This flat schema provides us the ability to view each nested xml field as a column
# df_flattened.printSchema

# COMMAND ----------

# de-duplication of data - keeps one record
df_deduped = df_flattened.dropDuplicates(['guid','mpan','event_timestamp','alert_code','end_time','received_timestamp'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Static Lookup Table (Alert Codes)
# MAGIC 
# MAGIC Before enriching the bronze data, we will load a static lookup table for our Alert codes.
# MAGIC Here, we'll use a csv file that contains Alert codes and their associated descriptions.

# COMMAND ----------

# Enforce alert code lookup table schema
lookup_schema = StructType([
    StructField("Alert Code", StringType(), True),
    StructField("DESC_TEXT", StringType(), True),
    StructField("Alert Type", StringType(), True),
    StructField("Final Desc", StringType(), True),
    StructField("Phase", StringType(), True),
    StructField("Phase Number", StringType(), True),
])

# COMMAND ----------

# Import alert codes lookup csv as dataframe
df_alertcode_lookup =  spark\
                       .read\
                       .format("csv")\
                       .option("header", True)\
                       .schema(lookup_schema)\
                       .load(bronze_alert_codes_lookup)\
                       .withColumnRenamed("Alert Code","alert_code")\
                       .withColumnRenamed("DESC_TEXT","alert_desc")\
                       .withColumnRenamed("Alert Type","alert_type")\
                       .withColumnRenamed("Final Desc","final_alert_desc")\
                       .withColumnRenamed("Phase","phase")\
                       .withColumnRenamed("Phase Number","phase_number")\
                       .withColumn('alert_code', regexp_replace('alert_code', '0x', ''))         #removing leading '0x' from alert code lookup

    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Static Customer Connectivity Table
# MAGIC 
# MAGIC Before enriching our bronze data, we will load a static customer connectivity table.
# MAGIC Here, we'll use a delta table from the Gold layer that contains customer connectivity data for the associated mpans

# COMMAND ----------

# read in customer connectivity table
df_customer_conn =(spark
                   .read
                   .format("delta")
                   .load("/mnt/gold/CustomerConnectivity")
                  )

# COMMAND ----------

#display(df_customer_conn)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Data

# COMMAND ----------

# Inner join voltage alerts streaming data with static alertcode lookup on 'alert_code' 
# Inner join volage alerts streaming data with static customer connectivity delta table on 'mpan'
# Add column 'dno' with conditions derived from 'mpan' column

df_joined = (df_deduped.join(df_alertcode_lookup, "alert_code", "inner")).join(df_customer_conn,"mpan","left_outer")\
            .withColumn('dno', when(col('mpan').substr(0,2) == "12", "LPN")
            .when(col('mpan').substr(0,2) == "19", "SPN")
            .when(col('mpan').substr(0,2) == "10", "EPN")
            .otherwise("Others"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output Data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Output View

# COMMAND ----------

# DBTITLE 1,Delta Table Merge
# Create function to merge incoming voltage alerts data into the existing voltage alerts delta table
def merge_delta(incoming, target):
    incoming.dropDuplicates(['guid','mpan','event_timestamp','alert_code','end_time','received_timestamp']).createOrReplaceTempView("incoming")
    

    # MERGE records into the target table using the specified join key
    incoming._jdf.sparkSession().sql(f"""
    MERGE INTO delta.`{target}` t
    USING incoming i
    ON i.mpan=t.mpan AND i.event_timestamp = t.event_timestamp AND i.received_timestamp= t.received_timestamp
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    
delta_table_exists = DeltaTable.isDeltaTable(spark,gold_path + "vw_voltage_alerts")

# Reset the output vw_voltage_alerts table
if not delta_table_exists:
    empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), df_joined.schema)
    empty_df.write.format("delta")\
                    .mode("overwrite")\
                    .partitionBy("event_year","event_month","event_day")\
                    .save(gold_path + "vw_voltage_alerts")
    
    # Create external hive table
    spark.sql(f'CREATE TABLE IF NOT EXISTS vw_voltage_alerts USING DELTA LOCATION "{gold_path + "vw_voltage_alerts"}"')

# Start the query to continuously upsert into vw_voltage_alerts table in update mode
merge_gold_stream = (
    df_joined
    .writeStream.format("delta")                                                        # Write the resulting stream
    .foreachBatch(lambda i, b: merge_delta(i, gold_path + "vw_voltage_alerts"))         # Pass each micro-batch to the merge_delta function
    .option("mergeSchema","true")                                                       # mergeSchema to create super-schema
    .outputMode("update")                                                               # Merge using update mode
    .option("checkpointLocation", gold_checkpoint_path  + "vw_voltage_alerts")          # Checkpoint to start streams in case of failure
    .start()
)

# COMMAND ----------

#%sql
#SELECT * FROM DELTA.`/mnt/gold/IIB/vw_voltage_alerts`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Output Voltage Alerts

# COMMAND ----------

# writing non-deduplicated data into gold delta table
write_to_delta_gold = ((df_flattened)
                  .writeStream.format('delta') 
                  .option("checkpointLocation", gold_checkpoint_path  + "voltage_alerts")     # Specify the location of checkpoint files & W-A logs
                  .outputMode("append")                                                       # add only new records to output sink
                  .option("mergeSchema","true")                                               # if schema changes for example new columns are added - will just merge the schemas from source to dest and create a 'super-schema'
                  .partitionBy("event_year","event_month","event_day")
                  .start(gold_path + "voltage_alerts")
)

# Create the external tables once data starts to stream in
while True:
    try:
        spark.sql(f'CREATE TABLE IF NOT EXISTS voltage_alerts USING DELTA LOCATION "{gold_path + "voltage_alerts"}"')
        # Get the number of new rows written
        #write_to_delta_gold.recentProgress[0]["numInputRows"]
        break
    except:
        pass

# COMMAND ----------

#%sql
#SELECT count(*) FROM DELTA.`/mnt/gold/IIB/voltage_alerts`

# COMMAND ----------

#%sql
#SELECT count(*) FROM DELTA.`/mnt/gold/IIB/vw_voltage_alerts`

# COMMAND ----------

#%sql
#SELECT * FROM DELTA.`/mnt/gold/IIB/vw_voltage_alerts`
#limit 100
