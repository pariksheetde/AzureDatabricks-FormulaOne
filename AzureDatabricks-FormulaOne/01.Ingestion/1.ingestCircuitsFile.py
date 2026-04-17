# Databricks notebook source
# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR CIRCUITS.CSV FILE
# MAGIC
# MAGIC ####----------------------------------------------------------------------------------
# MAGIC 1. Pass the parameter for the file name
# MAGIC 2. Ingest circuits.csv file
# MAGIC 3. Remove non numeric data from percentage
# MAGIC 4. Pivot the data by age group
# MAGIC 5. Join to dim_country to get the country, 3 digit country code and the total population.
# MAGIC
# MAGIC ####-----------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE THE PATHS FOR DIFFERENT ENVIRONMENTS

# COMMAND ----------

# DBTITLE 1,Include configuration file
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Set data source variable
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# DBTITLE 1,Print data source variable
print(v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR CIRCUITS.CSV

# COMMAND ----------

# DBTITLE 1,Schema definition for circuits data
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType

circuits_schema = StructType(fields = 
 [
  StructField("circuitId", IntegerType(), True),
  StructField("circuitRef", StringType(), True),
  StructField("name", StringType(), True),
  StructField("location", StringType(), True),
  StructField("country", StringType(), True),
  StructField("lat", DoubleType(), True),
  StructField("lng", DoubleType(), True),
  StructField("alt", DoubleType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST CIRCUITS.CSV FILE

# COMMAND ----------

# DBTITLE 1,Load and display circuits data
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_path}/circuits.csv")

display(circuits_df)
circuits_df.printSchema()
print(f"Number of Records Read {circuits_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT REQUIRED COLUMNS THAT NEEDS TO BE PROCESSED

# COMMAND ----------

# DBTITLE 1,Display Selected Circuit Data
from pyspark.sql.functions import col, lit

sel_circuits_df = circuits_df.select(
                                     col("circuitId").alias("circuit_id"), 
                                     col("circuitRef").alias("circuit_ref"),
                                     col("name"), "location", col("country"), 
                                     col("lat"), col("lng"), col("alt")
                                    )
display(sel_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Rename Circuit Data Columns
rename_circuits_df = sel_circuits_df.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("file_name", lit(v_data_source))

display(rename_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADD AUDIT COLUMNS

# COMMAND ----------

# DBTITLE 1,Import functions from included file
# MAGIC %run "../9.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Update timestamp in circuits data
from pyspark.sql.functions import current_timestamp
# circuits_final_df = rename_circuits_df.withColumn("load_dtm", current_timestamp())
circuits_final_df = ingest_dtm(rename_circuits_df)

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET

# COMMAND ----------

# DBTITLE 1,Write circuits data to data lake in parquet format
circuits_final_df.write.mode("overwrite").parquet(f"{processed_path}/circuits")
print(processed_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA THAT IS LOADED TO DATALAKE BACK INTO A DATAFRAME TO VERIFY THE WRITE WORKED

# COMMAND ----------

# DBTITLE 1,Load and validate circuits data
validate_circuits_df = spark.read \
.parquet(f"{processed_path}/circuits")

display(validate_circuits_df)
validate_circuits_df.printSchema()
print(f"NUMBER OF RECORDS TO BE PROCESSED: {validate_circuits_df.count()}")
print(processed_path)

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Circuits Load Confirmation Message
dbutils.notebook.exit("CIRCUITS LOADED IN PROCESSED CONTAINER")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PERFORM AUDIT INFORMATION TO LOG CIRCUITS LOAD

# COMMAND ----------

# DBTITLE 1,NA
# MAGIC %skip
# MAGIC import time
# MAGIC
# MAGIC start_time = time.time()
# MAGIC status = "Success"
# MAGIC error_message = None
# MAGIC
# MAGIC try:
# MAGIC     # Your main script logic
# MAGIC     df = spark.read.csv(f"{raw_path}/circuits.csv", header=True, inferSchema=True)
# MAGIC     record_count = df.count()
# MAGIC     # ... (other processing steps)
# MAGIC except Exception as e:
# MAGIC     status = "Failure"
# MAGIC     error_message = str(e)
# MAGIC     record_count = 0
# MAGIC
# MAGIC end_time = time.time()
# MAGIC duration = end_time - start_time
# MAGIC user_email = spark.sql("SELECT current_user()").collect()[0][0]
# MAGIC
# MAGIC spark.sql(f"""
# MAGIC   INSERT INTO formulaone_dev.bronze.audit_log
# MAGIC   VALUES (
# MAGIC     'circuit_ingest_script',
# MAGIC     {record_count},
# MAGIC     '{user_email}',
# MAGIC     TIMESTAMP('{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}'),
# MAGIC     TIMESTAMP('{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}'),
# MAGIC     {duration},
# MAGIC     '{status}',
# MAGIC     {f"'{error_message.replace('\'', '')}'" if error_message else 'NULL'}
# MAGIC   )
# MAGIC """)

# COMMAND ----------

# DBTITLE 1,Process Circuits Data and Log Audit Information
import time
from datetime import datetime

file_name = v_data_source.lower()
run_datetime = datetime.now()
batch_id = f"{run_datetime.strftime('%d-%m-%Y')}_{file_name}_processed"

start_time = time.time()
status = "Success"
error_message = None

try:
    df = spark.read.csv(f"{raw_path}/circuits.csv", header=True, inferSchema=True)
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
    'circuit_ingest_script',
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

# DBTITLE 1,Retrieve latest audit log data
# MAGIC %sql
# MAGIC -- TRUNCATE TABLE formulaone_dev.bronze.audit_log;
# MAGIC SELECT * FROM formulaone_dev.bronze.audit_log ORDER BY run_date DESC;

# COMMAND ----------

# DBTITLE 1,Exit Notebook - Validate Circuits Record Count
dbutils.notebook.exit(f"NUMBER OF RECORDS VALIDATED {validate_circuits_df.count()}")

# COMMAND ----------

import time
from datetime import datetime

# --- Metadata setup ---
script_name = "circuits_ingestion"
today_str = datetime.now().strftime('%d-%m-%Y')

# Get cluster_id and notebook_path
cluster_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().get()
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# --- Generate batch_id with daily sequence ---
result = spark.sql(f"""
    SELECT MAX(CAST(SPLIT(batch_id, '-')[-1] AS INT)) AS max_seq
    FROM formulaone_dev.bronze.audit_log
    WHERE batch_id LIKE '{today_str}-{script_name}-%'
""").collect()[0]['max_seq']
seq_num = 1 if result is None else result + 1
batch_id = f"{today_str}-{script_name}-{seq_num:02d}"

# --- Script execution and audit logging ---
start_time = time.time()
run_datetime = datetime.now()
status = "Success"
error_message = None

try:
    df = spark.read.csv(f"{raw_path}/circuits.csv", header=True, inferSchema=True)
    record_count = df.count()
except Exception as e:
    status = "Failure"
    error_message = str(e)
    record_count = 0

end_time = time.time()
duration = end_time - start_time
user_email = spark.sql("SELECT current_user()").collect()[0][0]

# --- Insert audit log ---
spark.sql(f"""
  INSERT INTO formulaone_dev.bronze.audit_log
  VALUES (
    '{batch_id}',
    '{script_name}',
    {record_count},
    '{user_email}',
    TIMESTAMP('{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}'),
    TIMESTAMP('{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}'),
    {duration},
    '{status}',
    {f"'{error_message.replace('\'', '')}'" if error_message else 'NULL'},
    TIMESTAMP('{run_datetime.strftime('%Y-%m-%d %H:%M:%S')}'),
    '{cluster_id}',
    '{notebook_path}'
  )
""")

# COMMAND ----------

dbutils.notebook.exit("DONE!")
