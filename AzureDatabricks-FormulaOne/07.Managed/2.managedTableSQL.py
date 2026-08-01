# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Notebook
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# DBTITLE 1,Load and Inspect Race Results Data Schema
race_results_df = spark.read.parquet(f"{presentation_path}/race_results")
# display(race_results_df)
race_results_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Create Managed Table for Race Results Data
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.race_results_managed_sql_v1;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.race_results_managed_sql_v1
# MAGIC (
# MAGIC race_year INT,
# MAGIC race_name STRING,
# MAGIC race_date timestamp,
# MAGIC circuit_location string,
# MAGIC driver_name string,
# MAGIC driver_number integer,
# MAGIC driver_nationality string,
# MAGIC team string,
# MAGIC grid integer,
# MAGIC fastest_lap integer,
# MAGIC race_time string,
# MAGIC points float,
# MAGIC position integer,
# MAGIC created_dt timestamp
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Insert Data into Race Results Managed Table
# MAGIC %sql
# MAGIC INSERT INTO f1_presentation.race_results_managed_sql_v1
# MAGIC SELECT * FROM f1_presentation.race_results_managed_python_v1;

# COMMAND ----------

# DBTITLE 1,Get Total Record Count from Race Results Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.race_results_managed_sql_v1;

# COMMAND ----------

# DBTITLE 1,Show Detailed Schema Information for Race Results Table
# MAGIC %sql
# MAGIC DESC EXTENDED f1_presentation.race_results_managed_sql_v1;

# COMMAND ----------

# DBTITLE 1,Complete Notebook Execution with Success Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
