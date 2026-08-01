# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Script
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# DBTITLE 1,Load Race Results Data from Parquet File
race_results_df = spark.read.parquet(f"{presentation_path}/race_results")
# display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOAD THE DATA INTO EXTERNAL TABLE

# COMMAND ----------

# DBTITLE 1,Save Race Results Dataframe as JSON and Register Table
race_results_df.write.mode("overWrite").format("json").option("path", f"{presentation_path}/external/race_results_ext_python_v1").saveAsTable("f1_presentation.race_results_ext_python_v1")

# COMMAND ----------

# DBTITLE 1,Display Full Race Results from Extended Python View
# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v1;

# COMMAND ----------

# DBTITLE 1,Count Total Records in Race Results Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC AS cnt
# MAGIC FROM f1_presentation.race_results_ext_python_v1;

# COMMAND ----------

# DBTITLE 1,Show Detailed Schema of Race Results Extended View
# MAGIC %sql
# MAGIC DESC EXTENDED f1_presentation.race_results_ext_python_v1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### VALIDATE THAT CORRECT DATA IN JSON FORMAT HAS BEEN WRITTEN TO EXTERNAL TABLE

# COMMAND ----------

# DBTITLE 1,Load and Preview Race Results Data from JSON File
validate_df = spark.read.json(f"{presentation_path}/external/race_results_ext_python_v1")
display(validate_df)
validate_df.count()

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Success Status Message
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
