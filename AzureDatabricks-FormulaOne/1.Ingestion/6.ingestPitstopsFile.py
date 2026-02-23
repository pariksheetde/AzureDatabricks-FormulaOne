# Databricks notebook source
# DBTITLE 1,Load Config File for Notebook
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Set Data Source Variable
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR PIT_STOPS.JSON FILE

# COMMAND ----------

# DBTITLE 1,Define Pit Stops Schema
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

pit_stops_schema = StructType(fields = 
 [
  StructField("driverId", IntegerType(), True),
  StructField("duration", StringType(), True),
  StructField("lap", IntegerType(), True),
  StructField("milliseconds", IntegerType(), True),
  StructField("raceId", IntegerType(), True),
  StructField("stop", StringType(), True),
  StructField("time", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST PIT_STOPS.JSON FILE

# COMMAND ----------

# DBTITLE 1,Load and Display Pit Stops Data
pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_path}/pit_stops.json")

display(pit_stops_df)
pit_stops_df.printSchema()
print(f"Number of Records Read {pit_stops_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Load Functions for Notebook
# MAGIC %run "../9.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Rename Pit Stops Columns and Add File Name
from pyspark.sql.functions import col, current_timestamp, lit, concat

pit_stops_renamed_df = ingest_dtm(pit_stops_df) \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("file_name", lit(v_data_source))

display(pit_stops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET

# COMMAND ----------

# DBTITLE 1,Save Processed Pit Stops Data as Parquet
pit_stops_renamed_df.write.mode("overwrite").parquet(f"{processed_path}/pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# DBTITLE 1,Load Pit Stops Data and Validate Record Count
validate_pit_stops_df = spark.read \
.parquet(f"{processed_path}/pit_stops")

display(validate_pit_stops_df)
validate_pit_stops_df.printSchema()
print(f"NUMBER OF RECORDS TO BE PROCESSED: {validate_pit_stops_df.count()}")
print(processed_path)

# COMMAND ----------

dbutils.notebook.exit("PITSTOPS LOADED IN PROCESSED CONTAINER")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PERFORM AUDIT INFORMATION TO LOG PITSTOPS LOAD

# COMMAND ----------

# DBTITLE 1,Process Pit_Stops from JSON File
import time
from datetime import datetime

file_name = v_data_source.lower()
run_datetime = datetime.now()
batch_id = f"{run_datetime.strftime('%d-%m-%Y')}_{file_name}_processed"

start_time = time.time()
status = "Success"
error_message = None

try:
    df = spark.read.option("multiLine", True).json(f"{raw_path}/pit_stops.json")
    record_count = df.count()
except Exception as e:
    status = "Failure"
    error_message = str(e)
    record_count = 0

end_time = time.time()
duration = end_time - start_time
user_email = spark.sql("SELECT current_user()").collect()[0][0]

spark.sql(f"""
  INSERT INTO formulaone_dev.bronze.audit_log
  VALUES (
    '{batch_id}',
    'pit_stops_ingest_script',
    {record_count},
    '{user_email}',
    TIMESTAMP('{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}'),
    TIMESTAMP('{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}'),
    {duration},
    '{status}',
    {f"'{error_message.replace('\'', '')}'" if error_message else 'NULL'},
    TIMESTAMP('{run_datetime.strftime('%Y-%m-%d %H:%M:%S')}')
  )
""")

# COMMAND ----------

# DBTITLE 1,Retrieve and Sort Audit Logs by Run Date
# MAGIC %sql
# MAGIC -- DELETE FROM formulaone_dev.bronze.audit_log WHERE script_name = 'pit_stops_ingest_script';
# MAGIC SELECT * FROM formulaone_dev.bronze.audit_log ORDER BY run_date;

# COMMAND ----------

# DBTITLE 1,Count Validated Pit Stops Records
dbutils.notebook.exit(f"NUMBER OF RECORDS VALIDATED: {validate_pit_stops_df.count()}")
