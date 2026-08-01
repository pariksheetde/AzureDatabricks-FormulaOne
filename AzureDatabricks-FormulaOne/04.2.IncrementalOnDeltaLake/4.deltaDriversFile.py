# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Script
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

# DBTITLE 1,Set and Retrieve File Name Parameter Using Widgets
dbutils.widgets.text("p_file_name", "")
v_file_name = dbutils.widgets.get("p_file_name")

# COMMAND ----------

# DBTITLE 1,Set and Retrieve File Date Widget Parameter
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Display the Current File Date Variable Value
print(v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR drivers.json FILE

# COMMAND ----------

# DBTITLE 1,Define schema for name fields with forename and surname
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

name_schema = StructType(fields = 
 [
  StructField("forename", StringType(), True),
  StructField("surname", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Define Spark Schema for Drivers DataFrame Structure
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType, DateType

drivers_schema = StructType(fields = 
 [
  StructField("code", StringType(), True),
  StructField("dob", DateType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("driverRef", StringType(), True),
  StructField("name", name_schema, True),
  StructField("nationality", StringType(), True),
  StructField("number", IntegerType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST constructors.json FILE

# COMMAND ----------

# DBTITLE 1,Load and Inspect Drivers Dataset with Schema and Count
drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_path}/drivers.json")

# display(drivers_df)
drivers_df.printSchema()
print(f"Number of Records Read {drivers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### EXPLODE THE COLUMNS TO EXTRACT COLUMNS FROM JSON OBJECT AS REQUIRED

# COMMAND ----------

# DBTITLE 1,Create Fullname and Add File Name Column to Drivers Dat ...

from pyspark.sql.functions import col, current_timestamp, lit, concat

explode_drivers_df = drivers_df.select(
                                       col("code"), col("dob"), col("driverid").alias("driver_id"), 
                                       col("name.forename"), col("name.surname"), col("name"),
                                       col("driverRef").alias("driver_ref"),
                                       col("nationality"), col("number")
                                       ) \
.withColumn("fullname", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("file_name", lit(v_file_name)) \
.drop("name")

# display(explode_drivers_df)

# COMMAND ----------

# DBTITLE 1,Include Common Helper Functions from External File
# MAGIC %run "../09.Includes/2.functions"

# COMMAND ----------

# DBTITLE 1,Select and Rename Key Columns in Drivers DataFrame
from pyspark.sql.functions import current_timestamp

drivers_final_df = ingest_dtm(explode_drivers_df) \
.select("driver_id", col("driver_ref"), 
                                             col("number"), "code", 
                                             col("fullname"),"dob",
                                             col("forename").alias("first_name"),
                                             col("surname").alias("last_name"),                                           
                                             col("nationality"),
                                             col("file_name")
                                            ) 

# display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOAD DRIVERS DATA INSIDE DELTA DB

# COMMAND ----------

# DBTITLE 1,Overwrite Drivers DataFrame to Delta Table Storage
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_delta.drivers")

# COMMAND ----------

# DBTITLE 1,Query Total Number of Records in Drivers Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt from f1_delta.drivers;

# COMMAND ----------

# DBTITLE 1,Confirm Successful Completion of Incremental Drivers Lo ...
dbutils.notebook.exit("INCREMENTAL LOAD FOR DRIVERS HAS BEEN LOADED SUCCESSFULLY")
