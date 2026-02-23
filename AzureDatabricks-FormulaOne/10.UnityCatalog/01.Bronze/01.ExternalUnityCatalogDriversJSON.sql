-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### LOAD `DRIVERS.JSON` AS EXTERNAL TABLE

-- COMMAND ----------

DROP TABLE IF EXISTS formulaone_dev.bronze.drivers;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS formulaone_dev.bronze.drivers
(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING,surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
LOCATION 'abfss://f1uc@formula1dbdevadls.dfs.core.windows.net/bronze/drivers.json';

-- COMMAND ----------

SELECT * FROM formulaone_dev.bronze.drivers

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.notebook.exit('EXITED SUCCESSFULLY')
