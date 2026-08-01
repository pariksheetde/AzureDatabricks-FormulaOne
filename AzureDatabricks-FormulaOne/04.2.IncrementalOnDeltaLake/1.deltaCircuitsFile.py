# Databricks notebook source
# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR circuits.csv FILE

# COMMAND ----------

# DBTITLE 1,Load Configuration Settings from External File
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Set Data Source Parameter for Notebook Widgets
dbutils.widgets.text("p_data_source", "CIRCUITS")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# DBTITLE 1,Display Current Data Source Variable Output
print(v_data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE DATE

# COMMAND ----------

# DBTITLE 1,Initialize File Date Parameter from Notebook Widget
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Print File Date Variable Output
print(v_file_date)

# COMMAND ----------

# DBTITLE 1,Define Schema for Circuits Data Structure
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

# DBTITLE 1,Load Circuits Data with Schema and Print Record Count
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_path}/incremental/{v_file_date}/circuits.csv")

# display(circuits_df)
circuits_df.printSchema()
print(f"Number of Records Read {circuits_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT REQUIRED COLUMNS

# COMMAND ----------

# DBTITLE 1,Select and Rename Columns in Circuits Dataframe
from pyspark.sql.functions import col, lit
sel_circuits_df = circuits_df.select(
                                     col("circuitId").alias("circuit_id"), 
                                     col("circuitRef").alias("circuit_ref"),
                                     col("name"), "location", col("country"), 
                                     col("lat"), col("lng"), col("alt")
                                    )
# display(sel_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Rename Circuits Columns and Add Source Metadata
rename_circuits_df = sel_circuits_df.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# display(rename_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADD AUDIT COLUMNS

# COMMAND ----------

# DBTITLE 1,- Import Utility Functions from External Script
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Add Ingestion Timestamp to Circuits DataFrame
from pyspark.sql.functions import current_timestamp
# circuits_final_df = rename_circuits_df.withColumn("load_dtm", current_timestamp())
circuits_final_df = ingest_dtm(rename_circuits_df)

# display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO DATALAKE

# COMMAND ----------

# DBTITLE 1,Save Processed Circuits Data as Delta Table with Overwr ...
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_delta.circuits")
# print(processed_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# DBTITLE 1,Query Total Circuit Records from Circuits Table
# MAGIC %sql 
# MAGIC SELECT COUNT(*) AS CNT FROM f1_delta.circuits;

# COMMAND ----------

# DBTITLE 1,Retrieve All Records from Circuits Table in F1 Delta
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.circuits;

# COMMAND ----------

# DBTITLE 1,Exit Notebook After Successful Circuits Incremental Loa ...
dbutils.notebook.exit("INCREMENTAL LOAD FOR CIRCUITSS HAS BEEN LOADED SUCCESSFULLY")
