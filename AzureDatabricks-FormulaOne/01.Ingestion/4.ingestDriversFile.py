# Databricks notebook source
# DBTITLE 1,Run Configuration File for Notebook
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Widget input retrieval from Databricks database
dbutils.widgets.text("p_file_name", "")
v_file_name = dbutils.widgets.get("p_file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define schema for drivers.json file

# COMMAND ----------

# DBTITLE 1,Define Name Schema Fields in PySpark
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

name_schema = StructType(fields = 
 [
  StructField("forename", StringType(), True),
  StructField("surname", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Define Drivers Schema Fields in PySpark
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

drivers_schema = StructType(fields = 
 [
  StructField("code", StringType(), True),
  StructField("dob", DateType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("driverRef", StringType(), True),
  StructField("name", name_schema, True),
  StructField("nationality", StringType(), True),
  StructField("number", IntegerType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST CONSTRUCTORS.JSON FILE

# COMMAND ----------

# DBTITLE 1,Load and Display Drivers Data
drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_path}/drivers.json")

display(drivers_df)
drivers_df.printSchema()
print(f"Number of Records Read {drivers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXPLODE THE COLUMNS TO EXTRACT COLUMNS FROM JSON OBJECT AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Transform and Display Exploded Drivers Data

from pyspark.sql.functions import col, current_timestamp, lit, concat

explode_drivers_df = drivers_df.select(
                                       col("code"), col("dob"), col("driverid").alias("driver_id"), 
                                       col("name.forename"), col("name.surname"), col("name"),
                                       col("driverRef").alias("driver_ref"),
                                       col("nationality"), col("number")
                                       ) \
.withColumn("fullname", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("file_name", lit(v_file_name)) \
.drop("name")

display(explode_drivers_df)

# COMMAND ----------

# DBTITLE 1,Run External Functions File
# MAGIC %run "../9.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Extract and Display Final Driver Details
from pyspark.sql.functions import current_timestamp

drivers_final_df = ingest_dtm(explode_drivers_df) \
.select("driver_id", col("driver_ref"), 
                                             col("number"), "code", 
                                             col("fullname"),"dob",
                                             col("forename").alias("first_name"),
                                             col("surname").alias("last_name"),                                           
                                             col("nationality"),
                                             col("file_name")
                                            ) 

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET

# COMMAND ----------

# DBTITLE 1,Save Processed Drivers Data to Parquet
drivers_final_df.write.mode("overwrite").parquet(f"{processed_path}/drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# DBTITLE 1,Load and Display Drivers Data Summary
validate_drivers_df = spark.read \
.parquet(f"{processed_path}/drivers")

display(validate_drivers_df)
validate_drivers_df.printSchema()
print(f"NUMBER OF RECORDS TO BE PROCESSED: {validate_drivers_df.count()}")


# COMMAND ----------

dbutils.notebook.exit("DRIVERS LOADED IN PROCESSED CONTAINER")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PERFORM AUDIT INFORMATION TO LOG DRIVERS LOAD

# COMMAND ----------

# DBTITLE 1,Notebook Audit Log Insertion and Data Ingestion
import time
from datetime import datetime

file_name = v_file_name.lower()
run_datetime = datetime.now()
batch_id = f"{run_datetime.strftime('%d-%m-%Y')}_{file_name}_processed"

start_time = time.time()
status = "Success"
error_message = None

try:
    df = spark.read.csv(f"{raw_path}/drivers.json")
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
    'drivers_ingest_script',
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
# MAGIC SELECT * FROM formulaone_dev.bronze.audit_log ORDER BY run_date DESC;

# COMMAND ----------

# DBTITLE 1,Exit Number of Validated Drivers Count
dbutils.notebook.exit(f"NUMBER OF RECORDS VALIDATED: {validate_drivers_df.count()}")
