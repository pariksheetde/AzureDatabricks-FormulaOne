-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### CREATE DATABASE FOR `FormulaOne`

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_etl CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_etl;

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_incremental CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_incremental
LOCATION "/mnt/formula1dbdevadls/incremental";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_delta CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_delta
LOCATION "/mnt/formula1dbdevadls/deltalake/Transformation";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation;

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE EXTERNAL LOCATION FOR BRONZE FOR UNITY CATALOG

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS formulaone_dev_adls_ext_bronze
URL 'abfss://f1uc@formula1dbdevadls.dfs.core.windows.net/bronze'
WITH (STORAGE CREDENTIAL `formulaone-dev-adls-storage-credential`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CHECK TO SEE IF `DRIVERS.JSON & RESULTS.JSON` ARE AVAILABLE IN THE BRONZE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %fs ls 'abfss://f1uc@formula1dbdevadls.dfs.core.windows.net/bronze'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE EXTERNAL LOCATION FOR SILVER

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS formulaone_dev_adls_ext_silver
URL 'abfss://f1uc@formula1dbdevadls.dfs.core.windows.net/silver'
WITH (STORAGE CREDENTIAL `formulaone-dev-adls-storage-credential`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE EXTERNAL LOCATION FOR GOLD

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS formulaone_dev_adls_ext_gold
URL 'abfss://f1uc@formula1dbdevadls.dfs.core.windows.net/gold'
WITH (STORAGE CREDENTIAL `formulaone-dev-adls-storage-credential`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE CATALOG FOR SCHEMA `BRONZE`, `SILVER`, `GOLD` 

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formulaone_dev;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATE SCHEMA `BRONZE`, `SILVER`, `GOLD`

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formulaone_dev.bronze MANAGED LOCATION 'abfss://f1uc@formula1dbdevadls.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formulaone_dev.silver MANAGED LOCATION 'abfss://f1uc@formula1dbdevadls.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formulaone_dev.gold MANAGED LOCATION 'abfss://f1uc@formula1dbdevadls.dfs.core.windows.net/gold';

-- COMMAND ----------

DROP TABLE IF EXISTS formulaone_dev.bronze.audit_log;
CREATE TABLE IF NOT EXISTS formulaone_dev.bronze.audit_log (
  batch_id STRING,
  script_name STRING,
  record_count LONG,
  user_email STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds DOUBLE,
  status STRING,
  error_message STRING,
  run_date TIMESTAMP
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("DATABASE CREATION EXECUTED SUCCESSFULLY")
