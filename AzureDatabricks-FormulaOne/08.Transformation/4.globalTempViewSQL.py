# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Dataframes using SQL
# MAGIC - 1. Create Global Temp viws on DataFrame
# MAGIC - 2. Access Views from SQL
# MAGIC - 3. Access Views from Python

# COMMAND ----------

# DBTITLE 1,Load Configuration Settings from External File
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# DBTITLE 1,Load and Display Race Results Dataframe from Parquet
races_results_df = spark.read.parquet(f"{presentation_path}/race_results")
display(races_results_df)

# COMMAND ----------

# DBTITLE 1,Create Global Temp View for Race Results Data
races_results_df.createOrReplaceGlobalTempView("Race_Results_Global_Temp_VW")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ACCESS VIEWS FROM SQL

# COMMAND ----------

# DBTITLE 1,Count of Races Grouped by Year in Descending Order
# MAGIC %sql
# MAGIC SELECT 
# MAGIC count(*) as no_of_races,
# MAGIC race_year
# MAGIC FROM global_temp.Race_Results_Global_Temp_VW
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year desc;

# COMMAND ----------

# MAGIC %md
# MAGIC #### ACCESS VIEWS FROM PYTHON

# COMMAND ----------

# DBTITLE 1,Summarize Race Counts Grouped by Year in Descending Ord ...
sql_qry = spark.sql("""SELECT 
count(*) as no_of_races, 
race_year 
FROM global_temp.Race_Results_Global_Temp_VW 
GROUP BY race_year 
ORDER BY race_year desc""")
display(sql_qry)

# COMMAND ----------

# DBTITLE 1,Finalize Notebook Execution with Success Message
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
