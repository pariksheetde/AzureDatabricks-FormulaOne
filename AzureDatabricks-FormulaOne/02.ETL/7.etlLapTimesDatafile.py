# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Script
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Setup Parameter Widget for Data Source Input
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR LAP_TIMES DIRECTORY

# COMMAND ----------

# DBTITLE 1,Define Schema for Laps Data Using StructType
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType

laps_schema = StructType(fields = 
 [
  StructField("raceId", IntegerType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("lap", IntegerType(), True),
  StructField("position", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST LAP_TIMES DIRECTORY

# COMMAND ----------

# DBTITLE 1,Load and Inspect Raw Lap Times Dataset
lap_times_df = spark.read \
.schema(laps_schema) \
.csv(f"{raw_path}/lap_times/lap_times_*")

# display(lap_times_df)
lap_times_df.printSchema()
print(f"Number of Records Read {lap_times_df.count()}")

print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Import Utility Functions from External Notebook
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Rename Lap Times Columns and Add Source File Info
from pyspark.sql.functions import col, current_timestamp, lit, concat, input_file_name

lap_times_renamed_df = ingest_dtm(lap_times_df) \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("source_file_name", input_file_name()) \
.withColumn("file_name", lit(v_data_source))

# display(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE LAP_TIMES DATA INSIDE PROCESSED DB

# COMMAND ----------

# DBTITLE 1,Save Renamed Lap Times Data as Parquet Table
lap_times_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_etl.lap_times")

# COMMAND ----------

# DBTITLE 1,Count Total Records in Lap Times Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_etl.lap_times;

# COMMAND ----------

# DBTITLE 1,Confirm Successful Loading of Lap Times in ETL Process
dbutils.notebook.exit("LAP TIMES HAS BEEN LOADED IN F1_ETL SUCCESSFULLY")
