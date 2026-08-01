# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Notebook
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Set and Retrieve Data Source Parameter Widget
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR results.json FILE

# COMMAND ----------

# DBTITLE 1,Define schema for race results data using PySpark Struc ...
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

results_schema = StructType(fields = 
 [
  StructField("constructorId", IntegerType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("fastestLap", IntegerType(), True),
  StructField("fastestLapSpeed", FloatType(), True),
  StructField("fastestLapTime", StringType(), True),
  StructField("grid", IntegerType(), True),
  StructField("laps", IntegerType(), True),
  StructField("milliseconds", IntegerType(), True),
  StructField("number", IntegerType(), True),
  StructField("points", FloatType(), True),
  StructField("position", IntegerType(), True),
  StructField("positionOrder", IntegerType(), True),
  StructField("positionText", StringType(), True),
  StructField("raceId", IntegerType(), True),
  StructField("rank", IntegerType(), True),
  StructField("resultId", IntegerType(), True),
  StructField("statusId", StringType(), True),
  StructField("time", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST results.json FILE

# COMMAND ----------

# DBTITLE 1,Load and Display Results Data with Schema Validation
results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_path}/results.json")

results_df.printSchema()
print(f"Number of Records Read {results_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Execute Shared Functions Notebook for Utilities
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Rename Columns and Add Source Info to Results Dataframe
from pyspark.sql.functions import col, current_timestamp, lit, concat

results_renamed_df = ingest_dtm(results_df) \
.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumn("file_name", lit(v_data_source)) \
.drop("statusId")

# display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE RESULTS DATA INSIDE PROCESSED DB

# COMMAND ----------

# DBTITLE 1,Write Results Dataframe to Partitioned Parquet Table
results_renamed_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_etl.partitioned_results")

# COMMAND ----------

# DBTITLE 1,Count Total Records in Formula One Results Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_etl.partitioned_results;

# COMMAND ----------

# DBTITLE 1,Confirm Successful Load Completion for F1 ETL Process
dbutils.notebook.exit("RESULTS HAS BEEN LOADED IN F1_ETL SUCCESSFULLY")
