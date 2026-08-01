# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from Includes Folder
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Set and Retrieve Data Source Parameter Using Widgets
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Define schema for pit_stops.json file

# COMMAND ----------

# DBTITLE 1,Define Schema for Pit Stops DataFrame
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

# MAGIC %md
# MAGIC #### INGEST results.json FILE

# COMMAND ----------

# DBTITLE 1,Load and Inspect Pit Stops Dataframe Schema and Count
pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_path}/pit_stops.json")

display(pit_stops_df)
pit_stops_df.printSchema()
print(f"Number of Records Read {pit_stops_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Run External Functions from Includes Directory
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Rename Columns and Add Data Source Tag to Pit Stops Dat ...
from pyspark.sql.functions import col, current_timestamp, lit, concat

pit_stops_renamed_df = ingest_dtm(pit_stops_df) \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("file_name", lit(v_data_source))

display(pit_stops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE PIT_STOPS DATA INSIDE PROCESSED DB

# COMMAND ----------

# DBTITLE 1,Save Pit Stops DataFrame as Parquet Table with Overwrit ...
pit_stops_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_etl.pit_stops")

# COMMAND ----------

# DBTITLE 1,Query Total Number of Records in Pit Stops Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_etl.pit_stops;

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Successful PIT Load Confirmation Mes ...
dbutils.notebook.exit("PIT HAS BEEN LOADED IN F1_ETL SUCCESSFULLY")
