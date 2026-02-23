# Databricks notebook source
# DBTITLE 1,Run configuration file for notebook.
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Widget input for data source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR LAP_TIMES DIRECTORY

# COMMAND ----------

# DBTITLE 1,Define Spark schema for race laps data
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

# DBTITLE 1,Load and Display Lap Times Data
lap_times_df = spark.read \
.schema(laps_schema) \
.csv(f"{raw_path}/lap_times/lap_times_*")

display(lap_times_df)
lap_times_df.printSchema()
print(f"NUMBER OF RECORDS TO BE PROCESSED: {lap_times_df.count()}")

print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Untitled 10/26/2021 14:30
# MAGIC %run "../9.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Rename columns and add file metadata in Lap Times data.
from pyspark.sql.functions import col, current_timestamp, lit, concat, input_file_name

lap_times_renamed_df = ingest_dtm(lap_times_df) \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("source_file_name", input_file_name()) \
.withColumn("file_name", lit(v_data_source))

display(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE IN PARQUET

# COMMAND ----------

# DBTITLE 1,Save Lap Times Data as Parquet File
lap_times_renamed_df.write.mode("overwrite").parquet(f"{processed_path}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# DBTITLE 1,Load and Display Lap Times Data Summary
validate_lap_times_df = spark.read \
.parquet(f"{processed_path}/lap_times")

display(validate_lap_times_df)
validate_lap_times_df.printSchema()
print(f"Number of Records Read {validate_lap_times_df.count()}")

# COMMAND ----------

dbutils.notebook.exit("LAP TIMES LOADED IN PROCESSED CONTAINER")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PERFORM AUDIT INFORMATION TO LOG LAPTIMES LOAD

# COMMAND ----------

# DBTITLE 1,Process Lap Times Data and Log Audit Trail
import time
from datetime import datetime

file_name = v_data_source.lower()
run_datetime = datetime.now()
batch_id = f"{run_datetime.strftime('%d-%m-%Y')}_{file_name}_processed"

start_time = time.time()
status = "Success"
error_message = None

try:
    df = spark.read.csv(f"{raw_path}/lap_times/lap_times_*")
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
    'lap_times_ingest_script',
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

# DBTITLE 1,Display Audit Log Data by Run Date
# MAGIC %sql
# MAGIC SELECT * FROM formulaone_dev.bronze.audit_log ORDER BY run_date DESC;

# COMMAND ----------

# DBTITLE 1,Exit with Validated Lap Times Count
dbutils.notebook.exit(f"NUMBER OF RECORDS VALIDATED: {validate_lap_times_df.count()}")
