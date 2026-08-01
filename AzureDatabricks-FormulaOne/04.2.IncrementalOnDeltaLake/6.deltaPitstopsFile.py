# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External File
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Retrieve and Display Data Source Parameter Value
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")        
print(v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE DATE

# COMMAND ----------

# DBTITLE 1,Set and Retrieve File Date Parameter for Processing
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")
print(v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR pit_stops.json FILE

# COMMAND ----------

# DBTITLE 1,Define Pit Stops DataFrame Schema with Spark Types
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

pit_stops_schema = StructType(fields = 
 [
  StructField("driverId", IntegerType(), True),
  StructField("duration", StringType(), True),
  StructField("lap", IntegerType(), True),
  StructField("milliseconds", IntegerType(), True),
  StructField("raceId", IntegerType(), True),
  StructField("stop", StringType(), True),
  StructField("time", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Display Current Raw Data File Path Variable
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST results.json FILE

# COMMAND ----------

# DBTITLE 1,Load Pit Stops Data with Schema and Record Count
pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_path}/incremental/{v_file_date}/pit_stops.json")

# display(pit_stops_df)
pit_stops_df.printSchema()
print(f"Number of Records Read {pit_stops_df.count()}")

print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Import Custom Utility Functions for Analysis
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Rename Pit Stops Columns Add Metadata and Count Records
from pyspark.sql.functions import col, current_timestamp, lit, concat

pit_stops_renamed_df = ingest_dtm(pit_stops_df) \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) 

# display(pit_stops_renamed_df)
print(f"Number of records {pit_stops_renamed_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET

# COMMAND ----------

# pit_stops_renamed_df.write.mode("append").partitionBy("race_id").parquet(f"{incremental_path}/pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# validate_pit_stops_df = spark.read \
# .parquet(f"{incremental_path}/pit_stops")

# display(validate_pit_stops_df)
# validate_pit_stops_df.printSchema()
# print(f"Number of Records Read {validate_pit_stops_df.count()}")

# COMMAND ----------

# DBTITLE 1,Select Key Pit Stop Metrics for Validation DataFrame
sel_validate_pit_stops_df = pit_stops_renamed_df.select(col("driver_id"), col("duration"), col("lap"), col("milliseconds"), col("stop"),
                                                        col("time"), col("load_ts"), col("file_name"), col("file_date"), col("race_id"))

# COMMAND ----------

# DBTITLE 1,Configure Dynamic Partition Overwrite Mode in Spark
# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# MAGIC %md
# MAGIC #### INCREMENTAL LOAD USING INSERTINTO()

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_incremental.pit_stops")):
#   sel_validate_pit_stops_df.write.mode("overwrite").insertInto("f1_incremental.pit_stops")
# else:
#   sel_validate_pit_stops_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_incremental.pit_stops")

# COMMAND ----------

# DBTITLE 1,Merge or Initialize Pit Stops Delta Table with Partitio ...
from delta.tables import DeltaTable
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

if (spark._jsparkSession.catalog().tableExists("f1_delta.pit_stops")):
  deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dbdevadls/deltalake/Transformation/pit_stops")
  deltaTable.alias("tgt").merge(
    sel_validate_pit_stops_df.alias("src"),
    "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()
else:
  sel_validate_pit_stops_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_delta.pit_stops")

# COMMAND ----------

# %sql
# SELECT
# race_id
# ,COUNT(race_id) as cnt
# FROM f1_delta.pit_stops
# GROUP BY race_id
# ORDER BY race_id DESC;

# COMMAND ----------

# DBTITLE 1,Preview Sample of Pit Stops Data from Delta Table
# MAGIC %sql
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM f1_delta.pit_stops LIMIT 25;

# COMMAND ----------

# DBTITLE 1,Aggregate Pit Stop Counts Grouped by File Date
# MAGIC %sql
# MAGIC SELECT 
# MAGIC file_date, 
# MAGIC COUNT(*) as CNT 
# MAGIC FROM f1_delta.pit_stops
# MAGIC GROUP BY 1;

# COMMAND ----------

# DBTITLE 1,Confirm Successful Incremental Load for Pit Stops Data
dbutils.notebook.exit("INCREMENTAL LOAD FOR PIT STOPS HAS BEEN LOADED SUCCESSFULLY")
