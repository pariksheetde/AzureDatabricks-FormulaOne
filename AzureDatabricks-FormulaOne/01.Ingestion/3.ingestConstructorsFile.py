# Databricks notebook source
# DBTITLE 1,Import Configuration File
# MAGIC %run "../9.Includes/1.config" 

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Set data source variable
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR CONSTRUCTORS.JSON FILE

# COMMAND ----------

# DBTITLE 1,Define Constructors Schema
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

constructor_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST CONSTRUCTORS.JSON FILE

# COMMAND ----------

# DBTITLE 1,Load constructors data from raw path
constructors_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_path}/constructors.json")

display(constructors_df)
constructors_df.printSchema()
print(f"Number of Records Read {constructors_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Function Invocation from Functions File
# MAGIC %run "../9.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Rename Columns and Add File Name
from pyspark.sql.functions import col, current_timestamp, lit

rename_constructors_df = ingest_dtm(constructors_df).withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("file_name", lit(v_data_source)) \
.drop(col("url"))

display(rename_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET

# COMMAND ----------

# DBTITLE 1,Write Data to Data Lake in Parquet Format
rename_constructors_df.write.mode("overwrite").parquet(f"{processed_path}/constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# DBTITLE 1,Load and Validate Constructors Data
validate_constructors_df = spark.read \
.parquet(f"{processed_path}/constructors")

display(validate_constructors_df)
validate_constructors_df.printSchema()
print(f"NUMBER OF RECORDS TO BE PROCESSED: {validate_constructors_df.count()}")

# COMMAND ----------

dbutils.notebook.exit("CONSTRUCTORS LOADED IN PROCESSED CONTAINER")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PERFORM AUDIT INFORMATION TO LOG CONSTRUCTORS LOAD

# COMMAND ----------

# DBTITLE 1,Process Constructors Data to Audit Log
import time
from datetime import datetime

file_name = v_data_source.lower()
run_datetime = datetime.now()
batch_id = f"{run_datetime.strftime('%d-%m-%Y')}_{file_name}_processed"

start_time = time.time()
status = "Success"
error_message = None

try:
    df = spark.read.csv(f"{raw_path}/constructors.json")
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
    'constructors_ingest_script',
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

# DBTITLE 1,Retrieve and Display Audit Log Data
# MAGIC %sql
# MAGIC -- TRUNCATE TABLE formulaone_dev.bronze.audit_log;
# MAGIC SELECT * FROM formulaone_dev.bronze.audit_log;

# COMMAND ----------

# DBTITLE 1,Exit Notebook After Validating Record Count
dbutils.notebook.exit(f"NUMBER OF RECORDS VALIDATED: {validate_constructors_df.count()}")
