# Databricks notebook source
# MAGIC %md
# MAGIC - History & Versioning
# MAGIC - Time Travel
# MAGIC - Vaccum

# COMMAND ----------

# DBTITLE 1,Describe Structure of Drivers Merge Table History
# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.drivers_merge;

# COMMAND ----------

# DBTITLE 1,Load Historical Drivers Data from Version Two
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge VERSION AS OF 2

# COMMAND ----------

# DBTITLE 1,Query Historical Snapshot of Drivers Merge Table Versio ...
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge VERSION AS OF 2

# COMMAND ----------

# DBTITLE 1,Show Schema Details for Drivers Merge History Table
# %sql
# DESC HISTORY f1_delta.drivers_merge

# COMMAND ----------

# DBTITLE 1,Create Drivers Transaction Table with Delta Format
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_delta.drivers_txn;
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.drivers_txn
# MAGIC (
# MAGIC driverid INT,
# MAGIC dob STRING,
# MAGIC firstname STRING,
# MAGIC lastname STRING,
# MAGIC created_dt DATE,
# MAGIC updated_dt DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Show Schema Details of Drivers Transaction History Tabl ...
# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.drivers_txn

# COMMAND ----------

# DBTITLE 1,Signal Successful Notebook Completion Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
