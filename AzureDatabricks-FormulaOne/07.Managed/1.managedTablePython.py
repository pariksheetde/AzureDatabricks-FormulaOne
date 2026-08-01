# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Notebook
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# DBTITLE 1,Load Race Results Data from Parquet File
race_results_df = spark.read.parquet(f"{presentation_path}/race_results")
# display(race_results_df)

# COMMAND ----------

# DBTITLE 1,Save Race Results DataFrame as Managed Parquet Table
race_results_df.write.mode("overWrite").format("parquet").saveAsTable("f1_presentation.race_results_managed_python_v1")

# COMMAND ----------

# DBTITLE 1,Load Complete Race Results from Managed View
# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results_managed_python_v1;

# COMMAND ----------

# DBTITLE 1,Get Total Count of Records in Race Results Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.race_results_managed_python_v1;

# COMMAND ----------

# DBTITLE 1,Inspect Schema Details of Managed Race Results Table
# MAGIC %sql
# MAGIC DESC EXTENDED f1_presentation.race_results_managed_python_v1;

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Successful Execution Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
