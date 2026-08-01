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

# DBTITLE 1,Set and Retrieve File Date Parameter
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Display Current File Date Parameter Value
print(v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR races.csv FILE

# COMMAND ----------

# DBTITLE 1,Define Spark Schema for Races Data Structure
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

# DBTITLE 1,Load and Inspect Races Dataset Schema and Record Count
races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_path}/races.csv")

# display(races_df)
races_df.printSchema()
print(f"Number of Records Read {races_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT REQUIRED COLUMNS

# COMMAND ----------

# DBTITLE 1,Select Key Columns from Races DataFrame for Analysis
from pyspark.sql.functions import col, lit
sel_races_df = races_df.select(
                               col("race_id"), col("year"), col("round"), "circuitid", col("name"), 
                               col("date"), col("time"), col("url")
                                    )
# display(sel_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME / DROP THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Refine Races DataFrame Rename Columns and Add Source
rename_races_df = sel_races_df.withColumnRenamed("circuitid", "circuit_id") \
.withColumnRenamed("year", "race_year") \
.drop(col("url")) \
.withColumn("file_name", lit(v_data_source))

# display(rename_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADD NEW COLUMNS

# COMMAND ----------

# DBTITLE 1,Import Common Functions and Utilities Script
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Create Race Timestamp Column by Combining Date and Time
# from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp, concat

# races_with_timestamp_df = ingest_dtm(rename_races_df).withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
# .drop("date", "time")

# display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE RACES DATA INSIDE DELTA DB

# COMMAND ----------

# DBTITLE 1,Overwrite and Save Races DataFrame as Partitioned Delta ...
rename_races_df.write.mode("overwrite").format("delta").partitionBy("race_id").saveAsTable("f1_delta.races")

# COMMAND ----------

# DBTITLE 1,Retrieve Total Race Count from Races Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt from f1_delta.races;

# COMMAND ----------

# DBTITLE 1,Confirm Successful Incremental Load for Races Dataset
dbutils.notebook.exit("INCREMENTAL LOAD FOR RACES HAS BEEN LOADED SUCCESSFULLY")
