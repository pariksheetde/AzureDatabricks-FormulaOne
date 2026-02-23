# Databricks notebook source
# DBTITLE 1,Load configuration file
# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Set data source parameter
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR RACES.CSV FILE

# COMMAND ----------

# DBTITLE 1,Define Races Schema
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

races_schema = StructType(fields = 
 [
  StructField("race_id", IntegerType(), True),
  StructField("year", IntegerType(), True),
  StructField("round", IntegerType(), True),
  StructField("circuitid", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("date", DateType(), True),
  StructField("time", StringType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST RACES.CSV FILE

# COMMAND ----------

# DBTITLE 1,Load raw races data
races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_path}/races.csv")

display(races_df)
races_df.printSchema()
print(f"Number of Records Read {races_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT REQUIRED COLUMNS

# COMMAND ----------

# DBTITLE 1,Display Selected Race Data
from pyspark.sql.functions import col, lit
sel_races_df = races_df.select(
                               col("race_id"), col("year"), col("round"), "circuitid", col("name"), 
                               col("date"), col("time"), col("url")
                                    )
display(sel_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME / DROP THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Renaming Race Data Columns
rename_races_df = sel_races_df.withColumnRenamed("circuitid", "circuit_id") \
.withColumnRenamed("year", "race_year") \
.drop(col("url")) \
.withColumn("file_name", lit(v_data_source))

display(rename_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADD NEW COLUMNS

# COMMAND ----------

# DBTITLE 1,Invoke functions for loading date and time
# MAGIC %run "../9.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,N/A
# MAGIC %skip
# MAGIC from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp, concat
# MAGIC
# MAGIC races_with_timestamp_df = ingest_dtm(rename_races_df).withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
# MAGIC .drop("date", "time")
# MAGIC
# MAGIC display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET

# COMMAND ----------

# DBTITLE 1,Write processed race data to Parquet format
rename_races_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_path}/races")
print(processed_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# DBTITLE 1,Load and Validate Races Data
validate_races_df = spark.read \
.parquet(f"{processed_path}/races")

display(validate_races_df)
validate_races_df.printSchema()
print(f"NUMBER OF RECORDS TO BE PROCESSED: {validate_races_df.count()}")

# COMMAND ----------

dbutils.notebook.exit("RACES LOADED IN PROCESSED CONTAINER")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PERFORM AUDIT INFORMATION TO LOG RACES LOAD

# COMMAND ----------

# DBTITLE 1,Process Races Data and Log Audit Info
import time
from datetime import datetime

file_name = v_data_source.lower()
run_datetime = datetime.now()
batch_id = f"{run_datetime.strftime('%d-%m-%Y')}_{file_name}_processed"

start_time = time.time()
status = "Success"
error_message = None

try:
    df = spark.read.csv(f"{raw_path}/races.csv", header=True, inferSchema=True)
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
    'races_ingest_script',
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

# DBTITLE 1,Audit Log Data Retrieval
# MAGIC %sql
# MAGIC -- TRUNCATE TABLE formulaone_dev.bronze.audit_log;
# MAGIC SELECT * FROM formulaone_dev.bronze.audit_log ORDER BY run_date DESC;

# COMMAND ----------

# DBTITLE 1,Count validated race records
dbutils.notebook.exit(f"NUMBER OF RECORDS VALIDATED {validate_races_df.count()}")
