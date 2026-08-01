# Databricks notebook source
# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR circuits.csv FILE
# MAGIC
# MAGIC ####----------------------------------------------------------------------------------
# MAGIC 1. Pass the parameter for the file name
# MAGIC 2. Ingest circuits.csv file
# MAGIC 3. Remove non numeric data from percentage
# MAGIC 4. Pivot the data by age group
# MAGIC 5. Join to dim_country to get the country, 3 digit country code and the total population.
# MAGIC
# MAGIC ####-----------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE THE PATHS FOR DIFFERENT ENVIRONMENTS

# COMMAND ----------

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

# DBTITLE 1,Display Current Raw Data File Path
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR circuits.csv

# COMMAND ----------

# DBTITLE 1,Define Schema for Circuits Dataset with Data Types
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType

circuits_schema = StructType(fields = 
 [
  StructField("circuitId", IntegerType(), True),
  StructField("circuitRef", StringType(), True),
  StructField("name", StringType(), True),
  StructField("location", StringType(), True),
  StructField("country", StringType(), True),
  StructField("lat", DoubleType(), True),
  StructField("lng", DoubleType(), True),
  StructField("alt", DoubleType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST circuits.csv FILE

# COMMAND ----------

# DBTITLE 1,Load Circuits CSV with Schema and Show Summary
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_path}/circuits.csv")

display(circuits_df)
circuits_df.printSchema()
print(f"Number of Records Read {circuits_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT REQUIRED COLUMNS THAT NEEDS TO BE PROCESSED

# COMMAND ----------

# DBTITLE 1,Select and Rename Key Columns from Circuits Dataset
from pyspark.sql.functions import col, lit

sel_circuits_df = circuits_df.select(
                                     col("circuitId").alias("circuit_id"), 
                                     col("circuitRef").alias("circuit_ref"),
                                     col("name"), "location", col("country"), 
                                     col("lat"), col("lng"), col("alt")
                                    )
display(sel_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Rename Circuit Coordinates and Add Data Source Column
rename_circuits_df = sel_circuits_df.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("file_name", lit(v_data_source))

display(rename_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADD NEW COLUMNS

# COMMAND ----------

# DBTITLE 1,Import Custom Functions from Shared Notebook
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Add Ingestion Timestamp to Circuits Dataframe and Displ ...
from pyspark.sql.functions import current_timestamp
# circuits_final_df = rename_circuits_df.withColumn("load_dtm", current_timestamp())
circuits_final_df = ingest_dtm(rename_circuits_df)

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE CIRCUITS DATA INSIDE PROCESSED DB

# COMMAND ----------

# DBTITLE 1,Save Circuits DataFrame as Parquet Table with Overwrite
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_etl.circuits")

# COMMAND ----------

# DBTITLE 1,Query Total Number of Circuits in Dataset
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_etl.circuits;

# COMMAND ----------

# DBTITLE 1,Confirm Successful Completion of Circuits Data Load
dbutils.notebook.exit("CIRCUITS HAS BEEN LOADED IN F1_ETL SUCCESSFULLY")
