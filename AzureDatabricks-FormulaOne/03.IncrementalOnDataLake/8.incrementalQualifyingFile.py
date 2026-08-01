# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External File
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Define Data Source Widget and Retrieve Parameter Value
dbutils.widgets.text("p_data_source", "qualifying")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE DATE

# COMMAND ----------

# DBTITLE 1,Set and Retrieve File Date Parameter from Widgets
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Display Current File Date Variable
print(v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR QUALIFYING FILE

# COMMAND ----------

# DBTITLE 1,Define Spark Schema for Formula One Qualifying Data
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

# DBTITLE 1,Load Qualifying Data and Display Schema Details
qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_path}/incremental/{v_file_date}/qualifying")

# display(qualifying_df)
qualifying_df.printSchema()
print(f"Number of Records Read {qualifying_df.count()}")
print(raw_path)

# COMMAND ----------

# DBTITLE 1,Run Shared Functions and Utility Code Definitions
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### REMAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Rename Qualifying Data Columns and Add Source Metadata
from pyspark.sql.functions import col, current_timestamp, lit, concat

qualifying_renamed_df = ingest_dtm(qualifying_df) \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

display(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET FILE

# COMMAND ----------

# DBTITLE 1,Save Qualifying Data Incrementally Partitioned by Race  ...
# qualifying_renamed_df.write.mode("append").partitionBy("race_id").parquet(f"{incremental_path}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# DBTITLE 1,Preview and Schema Check for Qualifying Data Load
# validate_qualifying_df = spark.read \
# .parquet(f"{incremental_path}/qualifying")

# display(validate_qualifying_df)
# validate_qualifying_df.printSchema()
# print(f"Number of Records Read {validate_qualifying_df.count()}")

# COMMAND ----------

# DBTITLE 1,Select and Prepare Final Qualifying Data Columns
qualifying_final_df = qualifying_renamed_df.select(col("constructor_id"), col("driver_id"), col("number"), col("position"), col("q1"),
                                            col("q2"), col("q3"), col("qualify_id"), col("load_ts"), col("file_name"), col("file_date"),
                                            col("race_id"))
# display(qualifying_final_df)

# COMMAND ----------

# DBTITLE 1,Set Spark Partition Overwrite Mode to Dynamic
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# DBTITLE 1,Write Qualifying Data with Conditional Table Creation
if (spark._jsparkSession.catalog().tableExists("f1_incremental.qualifying")):
  qualifying_final_df.write.mode("overwrite").insertInto("f1_incremental.qualifying")
else:
  qualifying_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_incremental.qualifying")

# COMMAND ----------

# DBTITLE 1,Count Qualifying Records Grouped by File Date
# MAGIC %sql
# MAGIC SELECT 
# MAGIC COUNT(*) as cnt,
# MAGIC file_date
# MAGIC FROM f1_incremental.qualifying
# MAGIC GROUP BY 2;

# COMMAND ----------

# DBTITLE 1,Exit Notebook After Successful Incremental Load Complet ...
dbutils.notebook.exit("INCREMENTAL LOAD FOR QUALIFYING HAS BEEN LOADED SUCCESSFULLY")
