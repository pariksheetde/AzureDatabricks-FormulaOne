# Databricks notebook source
# MAGIC %md
# MAGIC #### CREATE f1_presentation.constructors EXTERNAL TABLE
# MAGIC - Single Line JSON
# MAGIC - Simple structure

# COMMAND ----------

# DBTITLE 1,Create Formula One Constructors Table from JSON Data
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.constructors
# MAGIC (
# MAGIC constructorId INT,
# MAGIC constructorRef string,
# MAGIC name string,
# MAGIC nationality string,
# MAGIC url string
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/formula1dbdevadls/raw/constructors.json")

# COMMAND ----------

# DBTITLE 1,Display All Records from Constructors Table
# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructors;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE f1_presentation.drivers EXTERNAL TABLE
# MAGIC - Single Line JSON
# MAGIC - Simple Structure

# COMMAND ----------

# DBTITLE 1,Create Drivers Table from JSON Data Source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.drivers
# MAGIC (
# MAGIC code STRING,
# MAGIC dob DATE,
# MAGIC driverId INT,
# MAGIC driverRef STRING,
# MAGIC name STRUCT<forename: STRING, surname: STRING>,
# MAGIC nationality STRING,
# MAGIC number INT,
# MAGIC url string
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/formula1dbdevadls/raw/drivers.json")

# COMMAND ----------

# DBTITLE 1,Retrieve Driver Details from Drivers Table
# MAGIC %sql
# MAGIC SELECT 
# MAGIC code, 
# MAGIC dob, driverId, driverRef, 
# MAGIC name.forename, 
# MAGIC name.surname  
# MAGIC FROM f1_presentation.drivers;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE f1_presentation.results EXTERNAL TABLE
# MAGIC - Single Line JSON
# MAGIC - Simple Structure

# COMMAND ----------

# DBTITLE 1,Create Formula One Race Results Table from JSON
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.results;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.results
# MAGIC (
# MAGIC constructorId integer,
# MAGIC driverId integer,
# MAGIC fastestLap integer,
# MAGIC fastestLapSpeed float,
# MAGIC fastestLapTime string,
# MAGIC grid integer,
# MAGIC laps integer,
# MAGIC milliseconds integer,
# MAGIC number integer,
# MAGIC points float,
# MAGIC position integer,
# MAGIC positionOrder integer,
# MAGIC positionText string,
# MAGIC raceId integer,
# MAGIC rank integer,
# MAGIC resultId integer,
# MAGIC statusId string,
# MAGIC time string
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/formula1dbdevadls/raw/results.json")

# COMMAND ----------

# DBTITLE 1,Count Total Rows in Formula One Results Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.results;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE f1_presentation.pit_stops EXTERNAL TABLE
# MAGIC - Multi-Line JSON
# MAGIC - Simple Structure

# COMMAND ----------

# DBTITLE 1,Create Pit Stops Table from JSON Data Source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.pit_stops;
# MAGIC CREATE TABLE f1_presentation.pit_stops
# MAGIC (
# MAGIC driverId integer,
# MAGIC duration string,
# MAGIC lap integer,
# MAGIC milliseconds integer,
# MAGIC raceId integer,
# MAGIC stop string,
# MAGIC time string
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/formula1dbdevadls/raw/pit_stops.json", multiLine True)

# COMMAND ----------

# DBTITLE 1,Display All Records from Pit Stops Table
# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.pit_stops;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE f1_presentation.lap_times EXTERNAL TABLE
# MAGIC - CSV files
# MAGIC - Multiple files

# COMMAND ----------

# DBTITLE 1,Create Lap Times Table from CSV Data Source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.lap_times
# MAGIC (
# MAGIC raceId integer,
# MAGIC driverId integer,
# MAGIC lap integer,
# MAGIC position integer,
# MAGIC time string,
# MAGIC milliseconds integer
# MAGIC )
# MAGIC using csv
# MAGIC options (path "/mnt/formula1dbdevadls/raw/lap_times")

# COMMAND ----------

# DBTITLE 1,Count Total Rows in Lap Times Dataset
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.lap_times;

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE f1_presentation.qualifying EXTERNAL TABLE
# MAGIC - Multi-Line JSON file
# MAGIC - Multiple files

# COMMAND ----------

# DBTITLE 1,Create Qualifying Table from JSON Data Source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.qualifying
# MAGIC (
# MAGIC constructorId integer,
# MAGIC driverId integer,
# MAGIC number integer,
# MAGIC position integer,
# MAGIC q1 string,
# MAGIC q2 string,
# MAGIC q3 string,
# MAGIC qualifyId integer,
# MAGIC raceId integer
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/formula1dbdevadls/raw/qualifying/", multiLine True)

# COMMAND ----------

# DBTITLE 1,Count Total Rows in Qualifying Results Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.qualifying;

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Successful Execution Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
