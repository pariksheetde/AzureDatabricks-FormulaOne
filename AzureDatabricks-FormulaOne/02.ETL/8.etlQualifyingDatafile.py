# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Script
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
# MAGIC #### DEFINE SCHEMA FOR QUALIFYING DIRECTORY

# COMMAND ----------

# DBTITLE 1,Define Spark Schema for Qualifying Data Fields
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

# DBTITLE 1,Load and Inspect Qualifying Dataset from Raw JSON
qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_path}/qualifying")

# display(qualifying_df)
qualifying_df.printSchema()
print(f"Number of Records Read {qualifying_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Import Utility Functions from External Script
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Rename Columns and Add Data Source Tag to Qualifying Da ...
from pyspark.sql.functions import col, current_timestamp, lit, concat

qualifying_renamed_df = ingest_dtm(qualifying_df) \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("file_name", lit(v_data_source))

# display(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE QUALIFYING DATA INSIDE PROCESSED DB

# COMMAND ----------

# DBTITLE 1,Save Qualifying Dataframe to Parquet Table with Overwri ...
qualifying_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_etl.qualifying")

# COMMAND ----------

# DBTITLE 1,Query Total Count of Records in Qualifying Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_etl.qualifying;

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Qualifying Load Success Message
dbutils.notebook.exit("QUALIFYING HAS BEEN LOADED IN F1_ETL SUCCESSFULLY")
