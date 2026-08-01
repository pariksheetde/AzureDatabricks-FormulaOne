# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External File
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ 1st DAY'S DRIVER DATA

# COMMAND ----------

# DBTITLE 1,Load and Filter Driver Data with Selected Columns
from pyspark.sql.functions import col

driver_day1_df = spark.read.json(f"{raw_path}/incremental/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", col("name.forename").alias("firstname"), col("name.surname").alias("lastname"))

# display(driver_day1_df)
print(f"Number of Records {driver_day1_df.count()}")

# COMMAND ----------

# DBTITLE 1,Register Temporary View for Driver Day One Data
driver_day1_df.createOrReplaceTempView("driver_day1")

# COMMAND ----------

# DBTITLE 1,Load Complete Data from Driver Day One Table
# MAGIC %sql
# MAGIC SELECT * FROM driver_day1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ 2nd DAY'S DRIVER DATA

# COMMAND ----------

# DBTITLE 1,Load and Transform Driver Data for Specific ID Range
from pyspark.sql.functions import col, upper

driver_day2_df = spark.read.json(f"{raw_path}/incremental/2021-04-18/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper(col("name.forename")).alias("firstname"), upper(col("name.surname")).alias("lastname"))

# display(driver_day2_df)
print(f"Number of Records {driver_day2_df.count()}")

# COMMAND ----------

# DBTITLE 1,Register Temporary View for Driver Day Two Dataset
driver_day2_df.createOrReplaceTempView("driver_day2")

# COMMAND ----------

# DBTITLE 1,Load Complete Data from Driver Day Two Table
# MAGIC %sql
# MAGIC SELECT * FROM driver_day2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ 3rd DAY'S DRIVER DATA

# COMMAND ----------

# DBTITLE 1,Load and Normalize Driver Data for Selected ID Range
from pyspark.sql.functions import col, upper

driver_day3_df = spark.read.json(f"{raw_path}/incremental/2021-04-18/drivers.json") \
.filter("driverId BETWEEN 6 AND 15 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper(col("name.forename")).alias("firstname"), upper(col("name.surname")).alias("lastname"))

# display(driver_day3_df)
print(f"Number of Records {driver_day3_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE DELTA TABLE

# COMMAND ----------

# DBTITLE 1,Create Delta Table for Driver Merge Dataset
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_delta.driver_merge
# MAGIC (
# MAGIC driver_id INT,
# MAGIC dob DATE,
# MAGIC firstname STRING,
# MAGIC lastname STRING,
# MAGIC created_dt DATE,
# MAGIC updated_dt DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Load Full Dataset from Driver Merge Table
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.driver_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #### DAY 1 MERGE INTO TARGET TABLE

# COMMAND ----------

# DBTITLE 1,Merge Driver Day One Data into Delta Table with Upsert
# MAGIC %sql
# MAGIC MERGE INTO f1_delta.driver_merge tgt
# MAGIC USING driver_day1 src
# MAGIC ON (src.driverId = tgt.driver_id)
# MAGIC WHEN MATCHED 
# MAGIC   THEN UPDATE SET tgt.dob = src.dob,
# MAGIC              tgt.firstname = src.firstname,
# MAGIC              tgt.lastname = src.lastname,
# MAGIC              tgt.updated_dt = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (tgt.driver_id, firstname, lastname, tgt.created_dt) VALUES (src.driverid, src.firstname, src.lastname, CURRENT_TIMESTAMP)

# COMMAND ----------

# DBTITLE 1,Load Full Driver Merge Dataset from Delta Table
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.driver_merge tgt;

# COMMAND ----------

# MAGIC %md
# MAGIC #### DAY 2 MERGE INTO TARGET TABLE

# COMMAND ----------

# DBTITLE 1,Merge and Upsert Driver Records from Day Two Dataset
# MAGIC %sql
# MAGIC MERGE INTO f1_delta.driver_merge tgt
# MAGIC USING driver_day2 src
# MAGIC ON (src.driverId = tgt.driver_id)
# MAGIC WHEN MATCHED 
# MAGIC   THEN UPDATE SET tgt.dob = src.dob,
# MAGIC              tgt.firstname = src.firstname,
# MAGIC              tgt.lastname = src.lastname,
# MAGIC              tgt.updated_dt = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (tgt.driver_id, firstname, lastname, tgt.created_dt) VALUES (src.driverid, src.firstname, src.lastname, CURRENT_TIMESTAMP)

# COMMAND ----------

# DBTITLE 1,Load Full Driver Merge Dataset from F1 Delta Table
# MAGIC %sql
# MAGIC SELECT * FROM f1_delta.driver_merge tgt;

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Successful Execution Message
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
