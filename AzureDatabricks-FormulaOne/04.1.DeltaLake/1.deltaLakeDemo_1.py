# Databricks notebook source
# MAGIC %md
# MAGIC - 1.WRITE DATE TO DELTA LAKE (Managed Table)
# MAGIC - 2.WRITE DATE TO DELTA LAKE (External Table)
# MAGIC - 3.READ DATA FROM DELTA LAKE (Table)
# MAGIC - 4.READ DATA FROM DELTA LAKE (File)

# COMMAND ----------

# DBTITLE 1,Set and Retrieve Data Source Parameter Using Widgets
dbutils.widgets.text("p_data_source", "results")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# DBTITLE 1,Display Current Data Source Variable Value
print(v_data_source)

# COMMAND ----------

# DBTITLE 1,Define and Fetch File Date Parameter with Widgets
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Output Current File Date Variable Value
print(v_file_date)

# COMMAND ----------

# DBTITLE 1,Load and Prepare Incremental Results Data with Metadata
from pyspark.sql.functions import * 

results_df = spark.read \
.option("inferSchema", True) \
.json(f"/mnt/formula1dbdevadls/raw/incremental/{v_file_date}/results.json") \
.withColumn("load_ts", current_date()) \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) \
.orderBy("constructorId")

# display(results_df)
print(f"Number of records effected {results_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DELTA LAKE (Managed Delta Table)

# COMMAND ----------

# DBTITLE 1,Save Results DataFrame as Managed Delta Table
results_df.write.format("delta").mode("overwrite").saveAsTable("f1_delta.results_managed")

# COMMAND ----------

# DBTITLE 1,Count Records Grouped by File Date in Results Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT
# MAGIC FROM f1_delta.results_managed
# MAGIC GROUP BY file_date
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DELTA LAKE (External Delta Table)

# COMMAND ----------

# DBTITLE 1,Overwrite Delta Table with Updated Results DataFrame
results_df.write.format("delta").mode("overwrite").save("/mnt/formula1dbdevadls/deltalake/results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ DATA FROM DELTA LAKE (Table)

# COMMAND ----------

# DBTITLE 1,Create External Delta Table for Results Data Storage
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_delta.results_external;
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1dbdevadls/deltalake/results"

# COMMAND ----------

# DBTITLE 1,Aggregate Record Count by File Date in Results Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT
# MAGIC FROM f1_delta.results_external 
# MAGIC GROUP BY file_date
# MAGIC ORDER BY 1;

# COMMAND ----------

# DBTITLE 1,Show All Records from External Results Delta Table
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_delta.results_external

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ DATA FROM DELTA LAKE (File)

# COMMAND ----------

# DBTITLE 1,Load and Sort Formula One Results Data from Delta Lake
validate_results_df = spark.read.format("delta").load("/mnt/formula1dbdevadls/deltalake/results") \
.sort("constructorId")

# display(validate_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DML OPERATION ON DELTA LAKE (Update)

# COMMAND ----------

# DBTITLE 1,Reset Points to Zero for Specific Constructor in Result ...
# MAGIC %sql
# MAGIC UPDATE f1_delta.results_external
# MAGIC SET points = 0
# MAGIC WHERE constructorId = 1 

# COMMAND ----------

# DBTITLE 1,Query All Records for Constructor One from Results Tabl ...
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_external WHERE constructorid = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### UPDATE DELTA LAKE

# COMMAND ----------

# DBTITLE 1,Update Points for Constructor One in Results Delta Tabl ...
from delta.tables import *
from pyspark.sql.functions import *

results_delta_table_upd = DeltaTable.forPath(spark, "/mnt/formula1dbdevadls/deltalake/results")
results_delta_table_upd.update("constructorId = 1", {"points" : "100"})

# COMMAND ----------

# DBTITLE 1,Query All Records for Constructor One from External Res ...
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_external
# MAGIC WHERE constructorid = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### DELETE RECORDS FROM DELTA

# COMMAND ----------

# DBTITLE 1,Delete Records from Results Delta Table by Constructor  ...
from delta.tables import *
from pyspark.sql.functions import *

results_delta_table_del = DeltaTable.forPath(spark, "/mnt/formula1dbdevadls/deltalake/results")
results_delta_table_del.delete("constructorid = 1")

# COMMAND ----------

# DBTITLE 1,Query All Records for Constructor One from Results Tabl ...
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.results_external
# MAGIC WHERE constructorid = 1;

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Success Status Message
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
