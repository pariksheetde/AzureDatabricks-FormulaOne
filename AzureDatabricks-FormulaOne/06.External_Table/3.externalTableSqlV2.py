# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Notebook
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# DBTITLE 1,Load Race Results Dataset from Parquet File
race_results_df = spark.read.parquet(f"{presentation_path}/race_results")
# display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE EXTERNAL TABLE

# COMMAND ----------

# DBTITLE 1,Create External Table for Race Results Data in JSON For ...
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.race_results_ext_sql_v2;
# MAGIC CREATE EXTERNAL TABLE IF NOT EXISTS f1_presentation.race_results_ext_sql_v2
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
# MAGIC USING JSON
# MAGIC LOCATION "/mnt/formula1dbdevadls/presentation/external/race_results_ext_sql_v2"

# COMMAND ----------

# DBTITLE 1,Remove Existing Race Results Directory from Storage
# %fs rm -r "/mnt/presentation/external/race_results_ext_sql_v2"

# COMMAND ----------

# DBTITLE 1,Insert Race Results Data from Python to SQL Table
# MAGIC %sql
# MAGIC INSERT INTO f1_presentation.race_results_ext_sql_v2
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v2;

# COMMAND ----------

# DBTITLE 1,Count Total Records in Race Results External Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC AS total
# MAGIC FROM f1_presentation.race_results_ext_sql_v2;

# COMMAND ----------

# DBTITLE 1,Get Total Count of Records in Race Results Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.race_results_ext_python_v2;

# COMMAND ----------

# DBTITLE 1,Show Detailed Schema and Metadata for Race Results Tabl ...
# MAGIC %sql
# MAGIC DESC EXTENDED f1_presentation.race_results_ext_sql_v2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### VALIDATE THAT CORRECT DATA IN JSON FORMAT HAS BEEN WRITTEN TO EXTERNAL TABLE

# COMMAND ----------

# DBTITLE 1,Load and Count Records from Race Results JSON File
race_results_ext_sql_df = spark.read.json("/mnt/formula1dbdevadls/presentation/external/race_results_ext_sql_v2")
# display(race_results_ext_sql_df)
print(f"Number of Records Fetched {race_results_ext_sql_df.count()}")

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Successful Execution Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
