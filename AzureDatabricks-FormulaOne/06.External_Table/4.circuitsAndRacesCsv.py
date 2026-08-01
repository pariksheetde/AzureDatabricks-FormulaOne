# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Script
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE EXTERNAL TABLE FOR CIRCUITS

# COMMAND ----------

# DBTITLE 1,Create Circuits Table from CSV Data Source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.circuits
# MAGIC (
# MAGIC circuitId integer,
# MAGIC circuitRef string,
# MAGIC name string,
# MAGIC location string,
# MAGIC country string,
# MAGIC lat double,
# MAGIC lng double,
# MAGIC alt double,
# MAGIC url string
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "/mnt/formula1dbdevadls/raw/circuits.csv", header True)

# COMMAND ----------

# DBTITLE 1,Count Total Circuits in F1 Presentation Dataset
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE TABLE FOR RACES TABLE

# COMMAND ----------

# DBTITLE 1,Create Races Table with Schema from CSV File
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.races;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.races
# MAGIC (
# MAGIC race_id integer,
# MAGIC year integer,
# MAGIC round integer,
# MAGIC circuitid integer,
# MAGIC name string,
# MAGIC date date,
# MAGIC time string,
# MAGIC url string
# MAGIC )
# MAGIC using csv
# MAGIC options (path "/mnt/formula1dbdevadls/raw/races.csv", header True)

# COMMAND ----------

# DBTITLE 1,Calculate Total Number of Races in Dataset
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.races;

# COMMAND ----------

# DBTITLE 1,Finalize Notebook Execution with Success Exit Code
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
