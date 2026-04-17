# Databricks notebook source
# DBTITLE 1,Run Config File for Notebook
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
# MAGIC #### DEFINE SCHEMA FOR QUALIFYING DIRECTORY

# COMMAND ----------

# DBTITLE 1,Define Spark Schema for Qualifying Data
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

qualifying_schema = StructType(fields = 
 [
  StructField("constructorId", IntegerType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("number", IntegerType(), True),
  StructField("position", IntegerType(), True),
  StructField("q1", StringType(), True),
  StructField("q2", StringType(), True),
  StructField("q3", StringType(), True),
  StructField("qualifyId", IntegerType(), True),
  StructField("raceId", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST QUALIFYING DIRECTORY

# COMMAND ----------

# DBTITLE 1,Load and Display Qualifying Data
qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_path}/qualifying")

display(qualifying_df)
qualifying_df.printSchema()
print(f"Number of Records Read {qualifying_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Run Functions from Includes Folder
# MAGIC %run "../9.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Transform Qualifying Data Columns
from pyspark.sql.functions import col, current_timestamp, lit, concat

qualifying_renamed_df = ingest_dtm(qualifying_df) \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("file_name", lit(v_data_source))

display(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write data to DataLake as parquet

# COMMAND ----------

# DBTITLE 1,Save Qualifying Data as Parquet File
qualifying_renamed_df.write.mode("overwrite").parquet(f"{processed_path}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# DBTITLE 1,Load and Display Qualifying Data Summary
validate_qualifying_df = spark.read \
.parquet(f"{processed_path}/qualifying")

display(validate_qualifying_df)
validate_qualifying_df.printSchema()
print(f"NUMBER OF RECORDS TO BE PROCESSED: {validate_qualifying_df.count()}")

# COMMAND ----------

dbutils.notebook.exit("QUALIFYING LOADED IN PROCESSED CONTAINER")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PERFORM AUDIT INFORMATION TO LOG QUALIFYING LOAD

# COMMAND ----------

# DBTITLE 1,Process Qualifying Data Ingestion With Audit Log
import time
from datetime import datetime

file_name = v_data_source.lower()
run_datetime = datetime.now()
batch_id = f"{run_datetime.strftime('%d-%m-%Y')}_{file_name}_processed"

start_time = time.time()
status = "Success"
error_message = None

try:
    df = spark.read.option("multiLine", True).json(f"{raw_path}/qualifying")
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
    'qualifying_ingest_script',
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

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE formulaone_dev.bronze.audit_log;
# MAGIC SELECT * FROM formulaone_dev.bronze.audit_log ORDER BY run_date DESC;

# COMMAND ----------

# DBTITLE 1,Exit Validation Summary
dbutils.notebook.exit(f"NUMBER OF RECORDS VALIDATED: {validate_qualifying_df.count()}")
