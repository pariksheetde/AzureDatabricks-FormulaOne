# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Notebook
# MAGIC %run "../09.Includes/1.config" 

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Initialize Data Source Parameter Widget and Retrieve Va ...
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE DATE

# COMMAND ----------

# DBTITLE 1,Set File Date Parameter Using Databricks Widgets
dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Display Current File Date Variable Value
print(v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEFINE SCHEMA FOR constructors.json FILE

# COMMAND ----------

# DBTITLE 1,Define Constructor Schema with Data Types
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

constructor_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST constructors.json FILE

# COMMAND ----------

# DBTITLE 1,Load Constructors JSON and Display Schema Details
constructors_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_path}/incremental/{v_file_date}/constructors.json")

# display(constructors_df)
constructors_df.printSchema()
print(f"Number of Records Read {constructors_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Import Shared Utility Functions from External Notebook
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Transform Constructors Dataframe with Renaming and Meta ...
from pyspark.sql.functions import col, current_timestamp, lit

rename_constructors_df = ingest_dtm(constructors_df).withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) \
.drop(col("url"))

# display(rename_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE AS PARQUET

# COMMAND ----------

# DBTITLE 1,Save Updated Constructors DataFrame as Parquet File
# rename_constructors_df.write.mode("overwrite").parquet(f"{incremental_path}/constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# DBTITLE 1,Preview and Schema Check for Constructors Dataframe
# validate_constructors_df = spark.read \
# .parquet(f"{incremental_path}/constructors")

# display(validate_constructors_df)
# validate_constructors_df.printSchema()
# print(f"Number of Records Read {validate_constructors_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE CIRCUITS DATA INSIDE PROCESSED DATABASE

# COMMAND ----------

# DBTITLE 1,Overwrite and Save Constructors DataFrame as Parquet Ta ...
rename_constructors_df.write.mode("overwrite").format("parquet").saveAsTable("f1_incremental.constructors")

# COMMAND ----------

# DBTITLE 1,Retrieve All Records from Constructors Table
# MAGIC %sql
# MAGIC SELECT * FROM f1_incremental.constructors;

# COMMAND ----------

# DBTITLE 1,Count Total Records in Constructors Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_incremental.constructors;

# COMMAND ----------

dbutils.notebook.exit("INCREMENTAL LOAD FOR CONSTRUCTORS HAS BEEN LOADED SUCCESSFULLY")
