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
# MAGIC #### DEFINE SCHEMA FOR constructors.json FILE

# COMMAND ----------

# DBTITLE 1,Define Constructor Data Schema with Spark Types
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

constructor_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST constructors.json FILE

# COMMAND ----------

# DBTITLE 1,Load and Inspect Constructors Data from JSON File
constructors_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_path}/constructors.json")

display(constructors_df)
constructors_df.printSchema()
print(f"Number of Records Read {constructors_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Import Custom Functions from Shared Notebook Module
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Transform Constructors DataFrame Rename Columns and Add ...
from pyspark.sql.functions import col, current_timestamp, lit

rename_constructors_df = ingest_dtm(constructors_df).withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("file_name", lit(v_data_source)) \
.drop(col("url"))

display(rename_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE CIRCUITS DATA INSIDE PROCESSED DB

# COMMAND ----------

# DBTITLE 1,Save Renamed Constructors Dataframe to Parquet Table
rename_constructors_df.write.mode("overwrite").format("parquet").saveAsTable("f1_etl.constructors")

# COMMAND ----------

# DBTITLE 1,Count Total Records in Constructors Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt from f1_etl.constructors;

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Success Message for Constructors Loa ...
dbutils.notebook.exit("CONSTRUCTORS HAS BEEN LOADED IN F1_ETL SUCCESSFULLY")
