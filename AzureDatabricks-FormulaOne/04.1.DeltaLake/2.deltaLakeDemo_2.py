# Databricks notebook source
# MAGIC %md
# MAGIC - 1.WRITE DATA TO DELTA LAKE (Managed Table)
# MAGIC - 2.WRITE DATA TO DELTA LAKE (External table)
# MAGIC - 3.READ DATA FROM DELTA LAKE (Table)
# MAGIC - 4.READ DATA FROM DELTA LAKE (File)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT 1st DAY'S DATA

# COMMAND ----------

# DBTITLE 1,Load and Filter Top Drivers Data from JSON File
from pyspark.sql.functions import * 

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dbdevadls/raw/incremental/2021-03-28/drivers.json") \
.select("driverid", "dob", "name.forename", "name.surname") \
.filter("driverid <= 10") \
.orderBy("driverid")

# display(drivers_day1_df)

# COMMAND ----------

# DBTITLE 1,Register Temporary View for Day One Drivers Data
drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

# DBTITLE 1,Query All Records from Drivers Day 1 Table
# MAGIC %sql
# MAGIC SELECT * FROM drivers_day1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT 2nd DAY'S DATA

# COMMAND ----------

# DBTITLE 1,Load and Prepare Drivers Data with Uppercase Names
from pyspark.sql.functions import * 

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dbdevadls/raw/incremental/2021-03-28/drivers.json") \
.select("driverid", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname")) \
.filter("driverid BETWEEN 6 AND 15") \
.orderBy("driverid")

# display(drivers_day2_df)

# COMMAND ----------

# DBTITLE 1,Register Temporary View for Day Two Drivers Data
drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

# DBTITLE 1,Query Complete Dataset from Drivers Day Two Table
# MAGIC %sql
# MAGIC SELECT * FROM drivers_day2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT 3rd DAY'S DATA

# COMMAND ----------

# DBTITLE 1,Load and Filter Drivers Data with Name Uppercasing
from pyspark.sql.functions import * 

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dbdevadls/raw/incremental/2021-03-28/drivers.json") \
.select("driverid", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname")) \
.filter("driverid BETWEEN 1 AND 5 OR driverid BETWEEN 5 AND 20") \
.orderBy("driverid")

# display(drivers_day3_df)

# COMMAND ----------

# DBTITLE 1,Create Delta Table for Drivers Merge with Schema
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_delta.drivers_merge;
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.drivers_merge
# MAGIC (
# MAGIC driverid INT,
# MAGIC dob STRING,
# MAGIC forename STRING,
# MAGIC lastname STRING,
# MAGIC created_dt DATE,
# MAGIC updated_dt DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC #### DAY 1

# COMMAND ----------

# DBTITLE 1,Merge and Update Drivers Data from Day One Table
# MAGIC %sql
# MAGIC MERGE INTO f1_delta.drivers_merge tgt
# MAGIC USING drivers_day1 src
# MAGIC ON (src.driverid = tgt.driverid)
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = src.dob, 
# MAGIC              tgt.forename = src.forename, 
# MAGIC              tgt.lastname = src.surname, 
# MAGIC              tgt.updated_dt = CURRENT_TIMESTAMP()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (tgt.driverid, tgt.dob, tgt.forename, tgt.lastname, tgt.created_dt) VALUES (src.driverid, src.dob, src.forename, src.surname, CURRENT_TIMESTAMP())

# COMMAND ----------

# DBTITLE 1,Query All Records from Drivers Merge Delta Table
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #### DAY 2

# COMMAND ----------

# DBTITLE 1,Merge and Upsert Driver Records into Delta Table
# MAGIC %sql
# MAGIC MERGE INTO f1_delta.drivers_merge tgt
# MAGIC USING drivers_day2 src
# MAGIC ON (src.driverid = tgt.driverid)
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = src.dob, 
# MAGIC              tgt.forename = src.forename, 
# MAGIC              tgt.lastname = src.surname, 
# MAGIC              tgt.updated_dt = CURRENT_TIMESTAMP()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (tgt.driverid, tgt.dob, tgt.forename, tgt.lastname, tgt.created_dt) VALUES (src.driverid, src.dob, src.forename, src.surname, CURRENT_TIMESTAMP())

# COMMAND ----------

# DBTITLE 1,Query All Records from Drivers Merge Delta Table
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.drivers_merge;

# COMMAND ----------

# DBTITLE 1,Exit Notebook After Successful Execution Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
