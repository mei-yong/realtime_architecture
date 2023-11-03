# Databricks notebook source
# MAGIC %md
# MAGIC # Voltage Alerts (Master)
# MAGIC Objective: This is our master voltage alerts streaming pipeline. It executes notebooks using the notebook workflow capabilties of databricks. <br>
# MAGIC Author(s): annie.ho@baringa.com <br>
# MAGIC Date Updated: 2022-06-16 <br>
# MAGIC Assumptions:

# COMMAND ----------

# dbutils.notebook.run("voltage_alerts-source_to_bronze", 0)


# COMMAND ----------

# MAGIC %run "../Source-Bronze/voltage_alerts-source_to_bronze"

# COMMAND ----------

# MAGIC %run "../Bronze-Gold/voltage_alerts-bronze_to_gold"
