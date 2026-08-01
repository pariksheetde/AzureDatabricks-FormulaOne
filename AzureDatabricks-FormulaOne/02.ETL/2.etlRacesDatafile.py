# Databricks notebook source
# DBTITLE 1,Load Configuration Script for Notebook Setup
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
# MAGIC #### DEFINE SCHEMA FOR races.csv FILE

# COMMAND ----------

# DBTITLE 1,Define Schema for Races DataFrame with Typed Fields
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
# MAGIC #### INGEST races.csv FILE

# COMMAND ----------

# DBTITLE 1,Load and Inspect Races Data with Schema and Count
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

# DBTITLE 1,Select Key Columns from Races DataFrame for Display
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

# DBTITLE 1,Rename Columns and Add Data Source Identifier to Races  ...
rename_races_df = sel_races_df.withColumnRenamed("circuitid", "circuit_id") \
.withColumnRenamed("year", "race_year") \
.drop(col("url")) \
.withColumn("file_name", lit(v_data_source))

display(rename_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADD NEW COLUMNS

# COMMAND ----------

# DBTITLE 1,Run Helper Functions from External Script
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Create Race Timestamp Column by Merging Date and Time
# from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp, concat

# races_with_timestamp_df = ingest_dtm(rename_races_df).withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
# .drop("date", "time")

# display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE RACES DATA INSIDE PROCESSED DB

# COMMAND ----------

# DBTITLE 1,Write Races DataFrame to Partitioned Parquet Table
rename_races_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_etl.races")

# COMMAND ----------

# DBTITLE 1,Query Total Number of Records in Races Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt from f1_etl.races;

# COMMAND ----------

# DBTITLE 1,Confirm Successful Load of Races Data in F1 ETL Pipelin ...
dbutils.notebook.exit("RACES HAS BEEN LOADED IN F1_ETL SUCCESSFULLY")
