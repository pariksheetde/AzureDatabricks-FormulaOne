# Databricks notebook source
# DBTITLE 1,Set Current Database to f1 ETL Schema
# MAGIC %sql
# MAGIC use f1_etl;

# COMMAND ----------

# DBTITLE 1,Create View for Top Ten Race Results with Calculated Po ...
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW f1_presentation.calculated_race_results_vw
# MAGIC AS
# MAGIC SELECT
# MAGIC races.race_year,
# MAGIC constructors.name as team_name,
# MAGIC drivers.fullname as driver_name,
# MAGIC partitioned_results.position,
# MAGIC partitioned_results.points,
# MAGIC (11 - partitioned_results.position) as calculated_points
# MAGIC   FROM f1_etl.partitioned_results, f1_etl.drivers, f1_etl.constructors, f1_etl.races
# MAGIC   WHERE partitioned_results.driver_id = drivers.driver_id
# MAGIC   AND partitioned_results.constructor_id = constructors.constructor_id
# MAGIC   AND partitioned_results.race_id = races.race_id
# MAGIC AND partitioned_results.position <= 10;

# COMMAND ----------

# DBTITLE 1,Load Complete Race Results from Calculated View
# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.calculated_race_results_vw;

# COMMAND ----------

# DBTITLE 1,Count Total Records in Calculated Race Results View
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM f1_presentation.calculated_race_results_vw;

# COMMAND ----------

# DBTITLE 1,Exit Notebook Indicating Successful Execution
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
