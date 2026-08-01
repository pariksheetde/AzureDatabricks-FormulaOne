# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Dataframes using SQL
# MAGIC - 1. Create Temp viws on DataFrame
# MAGIC - 2. Access Views from SQL
# MAGIC - 3. Access Views from Python

# COMMAND ----------

# DBTITLE 1,Load Configuration Settings from External Notebook
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# DBTITLE 1,Load Race Results Data from Parquet File
races_results_df = spark.read.parquet(f"{presentation_path}/race_results")
# display(races_results_df)

# COMMAND ----------

# DBTITLE 1,Create Temporary View for Race Results Data
races_results_df.createOrReplaceTempView("Race_Results_Temp_VW")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ACCESS VIEWS FROM SQL

# COMMAND ----------

# DBTITLE 1,Summarize Number of Races by Year in Descending Order
# MAGIC %sql
# MAGIC SELECT 
# MAGIC count(*) as no_of_races,
# MAGIC race_year
# MAGIC FROM race_results_temp_vw
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year desc;

# COMMAND ----------

# MAGIC %md
# MAGIC #### ACCESS VIEWS FROM PYTHON

# COMMAND ----------

# DBTITLE 1,Aggregate Total Races Per Year Sorted by Descending Yea ...
race_results_df = spark.sql("""SELECT 
count(*) as no_of_races, 
race_year 
FROM race_results_temp_vw 
GROUP BY race_year 
ORDER BY race_year desc""")

# display(race_results_df)

# COMMAND ----------

# DBTITLE 1,Send Successful Execution Signal to Caller Notebook
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %md
# MAGIC #### GLOBAL TEMP VIEW CAN BE EXECUTED FROM OTHER NOTEBOOK
