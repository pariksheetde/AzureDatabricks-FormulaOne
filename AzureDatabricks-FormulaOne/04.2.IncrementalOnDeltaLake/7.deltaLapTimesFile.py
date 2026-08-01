# Databricks notebook source
# DBTITLE 1,Load Configuration Script for Notebook Setup
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Get and Print Data Source Widget Value
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
print(v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE DATE

# COMMAND ----------

# DBTITLE 1,Set And Retrieve File Date Widget Parameter
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")
print(v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR LAP_TIMES DIRECTORY

# COMMAND ----------

# DBTITLE 1,Define Spark Schema for Race Lap Data Structure
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

# DBTITLE 1,Load and Inspect Lap Times Data from Incremental Folder
lap_times_df = spark.read \
.schema(laps_schema) \
.csv(f"{raw_path}/incremental/{v_file_date}/lap_times/lap_times_*")

# display(lap_times_df)
lap_times_df.printSchema()
print(f"Number of Records Read {lap_times_df.count()}")

print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Execute Functions Script for Utility Definitions
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Rename Lap Times Columns and Add Source Metadata
from pyspark.sql.functions import col, current_timestamp, lit, concat

lap_times_renamed_df = ingest_dtm(lap_times_df) \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# display(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET

# COMMAND ----------

# lap_times_renamed_df.write.mode("append").partitionBy("race_id").parquet(f"{incremental_path}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# validate_lap_times_df = spark.read \
# .parquet(f"{incremental_path}/lap_times")

# display(validate_lap_times_df)
# validate_lap_times_df.printSchema()
# print(f"Number of Records Read {validate_lap_times_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE LAP_TIMES DATA INSIDE INCREMENTAL DB

# COMMAND ----------

# DBTITLE 1,Select and Validate Final Lap Times Dataframe Columns
validate_final_lap_times_df = lap_times_renamed_df.select(col("driver_id"), col("lap"), col("position"), col("time"), col("milliseconds"),
                                                          col("load_ts"), col("file_name"), col("file_date"), col("race_id"))

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_incremental.lap_times")):
#   validate_final_lap_times_df.write.mode("overwrite").insertInto("f1_incremental.lap_times")
# else:
#   validate_final_lap_times_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_incremental.lap_times")

# COMMAND ----------

# DBTITLE 1,Merge and Update Lap Times Data into Delta Table
from delta.tables import DeltaTable
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

if (spark._jsparkSession.catalog().tableExists("f1_delta.lap_times")):
  deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dbdevadls/deltalake/Transformation/lap_times")
  deltaTable.alias("tgt").merge(
    validate_final_lap_times_df.alias("src"),
    "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.lap = src.lap") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()
else:
  validate_final_lap_times_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_delta.lap_times")

# COMMAND ----------

# %sql
# SELECT
# race_id
# ,COUNT(*) as cnt FROM 
# f1_delta.lap_times
# GROUP BY race_id
# ORDER BY race_id DESC;

# COMMAND ----------

# DBTITLE 1,Group Lap Times by File Date with Count Aggregation
# MAGIC %sql
# MAGIC SELECT
# MAGIC file_date,
# MAGIC COUNT(*) as cnt 
# MAGIC FROM 
# MAGIC f1_delta.lap_times
# MAGIC GROUP BY 1;

# COMMAND ----------

dbutils.notebook.exit("INCREMENTAL LOAD FOR LAP TIMES HAS BEEN LOADED SUCCESSFULLY")
