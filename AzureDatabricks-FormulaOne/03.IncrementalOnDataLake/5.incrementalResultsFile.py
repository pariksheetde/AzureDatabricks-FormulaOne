# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Notebook
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Set Input Widget for Data Source Parameter
dbutils.widgets.text("p_data_source", "results")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE DATE

# COMMAND ----------

# DBTITLE 1,Set and Retrieve File Date Parameter from Widget
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Define File Date Variable for Dataset Versioning
v_file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE THE SCHEMA FOR results.json FILE

# COMMAND ----------

# DBTITLE 1,Define Results Data Schema with Detailed Race Fields
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
# MAGIC #### READ results.json FILE

# COMMAND ----------

# DBTITLE 1,Read and Analyze Incremental Results Data File
results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_path}/incremental/{v_file_date}/results.json")

# display(results_df)
results_df.printSchema()
print(f"Number of Records Read {results_df.count()}")

print(raw_path)

# COMMAND ----------

# DBTITLE 1,Load Utility Functions from Shared Notebook
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Rename Results Data Columns and Add Metadata Fields
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
.withColumn("file_date", lit(v_file_date)) \
.drop("statusId")

# display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT THE REQUIRED COLUMNS

# COMMAND ----------

# DBTITLE 1,Select and Display Final Race Results with Key Metrics
results_final_df = results_renamed_df.select(col("constructor_id"), col("driver_id"), col("fastest_lap"), col("fastest_lap_speed"), col("fastest_lap_time"),
                                            col("grid"), col("laps"), col("milliseconds"), col("number"), col("points"), col("position"),
                                            col("position_order"), col("position_text"), col("rank"), col("result_id"), col("time"), col("load_ts"),
                                            col("file_name"), col("file_date"), col("race_id"))
# display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE THE DATA TO THE DATALAKE AS PARQUET FILE

# COMMAND ----------

# DBTITLE 1,Remove Existing Partitions for Each Race in Results Tab ...
for race_id_list in results_final_df.select("race_id").distinct().collect():
  if (spark._jsparkSession.catalog().tableExists("f1_incremental.results")):
    spark.sql(f"ALTER TABLE f1_incremental.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# DBTITLE 1,Save Results DataFrame as Partitioned Parquet Files
# results_final_df.write.mode("append").partitionBy("race_id").parquet(f"{incremental_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO THE DATALAKE BACK TO THE DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# DBTITLE 1,Preview and Schema Check for Driver Validation Data
# validate_drivers_df = spark.read \
# .parquet(f"{incremental_path}/results")

# display(validate_drivers_df)
# validate_drivers_df.printSchema()
# print(f"Number of Records Read {validate_drivers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### INCREMENTAL LOAD USING append()

# COMMAND ----------

# DBTITLE 1,Append Results Data to Partitioned Parquet Table by Rac ...
results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_incremental.results")

# COMMAND ----------

# DBTITLE 1,Summarize Record Counts Grouped by File Date in Results
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   file_date, 
# MAGIC   count(file_date) as cnt
# MAGIC   FROM f1_incremental.results
# MAGIC   GROUP BY file_date
# MAGIC   ORDER BY file_date DESC;

# COMMAND ----------

dbutils.notebook.exit("INCREMENTAL LOAD FOR RESULTS HAS BEEN LOADED SUCCESSFULLY")
