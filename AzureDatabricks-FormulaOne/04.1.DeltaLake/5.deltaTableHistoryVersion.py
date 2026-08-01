# Databricks notebook source
# DBTITLE 1,Describe Structure of f1 delta driver merge History Tab ...
# MAGIC %sql
# MAGIC DESC HISTORY f1_delta.driver_merge;

# COMMAND ----------

# DBTITLE 1,Load Historical Snapshot of Driver Merge Table Version  ...
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.driver_merge VERSION AS OF 2;

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Success Status Message
dbutils.notebook.exit("SUCCESSFULLY EXECUTED")
