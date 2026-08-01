# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from Includes Directory
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA FROM PRESENTATION LAYER

# COMMAND ----------

# DBTITLE 1,Load Race Results Data and Display Record Count
race_results_df = spark.read.parquet("/mnt/formula1dbdevadls/presentation/race_results")
# display(race_results_df)
print(f"Number of records fetched {race_results_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE THE DATA TO EXTERNAL TABLE USING PYTHON

# COMMAND ----------

# DBTITLE 1,Save race results dataframe to presentation external ta ...
race_results_df.write.mode("overwrite").option("path",f"{presentation_path}/external/race_results_ext_python_v2").saveAsTable("f1_presentation.race_results_ext_python_v2")

# COMMAND ----------

# DBTITLE 1,Get Total Count of Records in Race Results Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM f1_presentation.race_results_ext_python_v2;

# COMMAND ----------

# DBTITLE 1,Preview Single Record from Race Results Table
# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v2 LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE THE DATA TO THE MANAGED TABLE USING SQL

# COMMAND ----------

# DBTITLE 1,Create External Table for Extended Race Results Dataset
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.race_results_ext_sql_v1;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.race_results_ext_sql_v1
# MAGIC (
# MAGIC race_year INT,
# MAGIC race_name STRING,
# MAGIC race_date TIMESTAMP,
# MAGIC circuit_location STRING,
# MAGIC driver_name STRING,
# MAGIC driver_number INT,
# MAGIC driver_nationality STRING,
# MAGIC team STRING,
# MAGIC grid INT,
# MAGIC fastest_lap INT,
# MAGIC race_time STRING,
# MAGIC points INT,
# MAGIC position INT,
# MAGIC created_dt TIMESTAMP
# MAGIC )
# MAGIC USING CSV
# MAGIC LOCATION "/mnt/formula1dbdevadls/presentation/external/race_results_ext_sql_v1"

# COMMAND ----------

# %fs rm -r "/mnt/formula1dbdevadls/presentation/external/race_results_ext_sql_v1"

# COMMAND ----------

# DBTITLE 1,Refresh External Race Results Table for Latest Data
# MAGIC %sql
# MAGIC REFRESH TABLE f1_presentation.race_results_ext_sql_v1;

# COMMAND ----------

# DBTITLE 1,Refresh Extended Race Results Presentation Table
# MAGIC %sql
# MAGIC REFRESH TABLE f1_presentation.race_results_ext_python_v1;

# COMMAND ----------

# DBTITLE 1,Insert Data into Extended Race Results Table
# MAGIC %sql
# MAGIC INSERT INTO f1_presentation.race_results_ext_sql_v1
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### VALIDATE EXTERNAL TABLE IS LOADED WITH ACCURATE ROW COUNT

# COMMAND ----------

# DBTITLE 1,Fetch Total Number of Records from Race Results Dataset
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS cnt FROM f1_presentation.race_results_ext_sql_v1;

# COMMAND ----------

# DBTITLE 1,Load External Race Results CSV and Count Records
validate_ext_sql_df = spark.read \
.option("header", False) \
.csv("/mnt/formula1dbdevadls/presentation/external/race_results_ext_sql_v1") 
        
# display(validate_ext_sql_df)
print(validate_ext_sql_df.count())

# COMMAND ----------

# DBTITLE 1,Show Extended Schema Details for Race Results View
# MAGIC %sql
# MAGIC DESC EXTENDED f1_presentation.race_results_ext_sql_v1;

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Successful Execution Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
