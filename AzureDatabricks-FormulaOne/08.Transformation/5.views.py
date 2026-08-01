# Databricks notebook source
# MAGIC %md
# MAGIC ##### CREATE TEMP VIEW

# COMMAND ----------

# DBTITLE 1,Create Temporary View for 2020 Formula One Race Results
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP view race_results_temp_view
# MAGIC AS
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v1 where race_year = 2020;

# COMMAND ----------

# DBTITLE 1,Display All Records from Race Results Temporary View
# MAGIC %sql
# MAGIC SELECT * FROM race_results_temp_view;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CREATE GLOBAL TEMP VIEW

# COMMAND ----------

# DBTITLE 1,Create Global Temp View for 2019 Formula One Results
# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP view race_results_temp_global_view
# MAGIC AS
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v2 where race_year = 2019;

# COMMAND ----------

# DBTITLE 1,Show Complete Data from Global Race Results Temporary V ...
# MAGIC %sql
# MAGIC SELECT * FROM global_temp.race_results_temp_global_view;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### CREATE PERMANENT VIEW

# COMMAND ----------

# DBTITLE 1,Create Combined View for 2019 and 2020 Race Results
# MAGIC %sql
# MAGIC CREATE OR REPLACE view f1_presentation.race_results_view
# MAGIC AS
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v1 WHERE race_year IN (2019)
# MAGIC UNION
# MAGIC SELECT * FROM f1_presentation.race_results_ext_python_v2 WHERE race_year IN (2020);

# COMMAND ----------

# DBTITLE 1,Retrieve All Formula One Race Results Ordered by Year
# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results_view ORDER BY race_year asc;

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Successful Execution Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
