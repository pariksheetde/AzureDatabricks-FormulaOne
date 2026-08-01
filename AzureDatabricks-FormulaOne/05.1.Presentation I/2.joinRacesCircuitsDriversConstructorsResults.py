# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Notebook
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ FROM RACES DATASET

# COMMAND ----------

# DBTITLE 1,Load and Rename Columns in Races DataFrame with Count
races_df = spark.read \
.parquet(f"{processed_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")

display(races_df)
print(f"Number of Records Read {races_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ FROM CIRCUITS DATASET

# COMMAND ----------

# DBTITLE 1,Load and Rename Columns in Circuits DataFrame with Coun ...
circuit_df = spark.read \
.parquet(f"{processed_path}/circuits") \
.withColumnRenamed("location", "circuit_location")

display(circuit_df)
print(f"Number of Records Read {circuit_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ FROM DRIVERS DATASET

# COMMAND ----------

# DBTITLE 1,Load and Rename Columns in Drivers DataFrame with Count
drivers_df = spark.read \
.parquet(f"{processed_path}/drivers") \
.withColumnRenamed("fullname", "driver_name") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("nationality", "driver_nationality")

display(drivers_df)
print(f"Number of Records Read {drivers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ FROM CONSTRUCTORS DATASET

# COMMAND ----------

# DBTITLE 1,Load and Rename Constructors DataFrame with Record Coun ...
constructors_df = spark.read \
.parquet(f"{processed_path}/constructors") \
.withColumnRenamed("constructor_id", "cons_id") \
.withColumnRenamed("name", "team")

display(constructors_df)
print(f"Number of Records Read {constructors_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ FROM RESULTS DATASET

# COMMAND ----------

# DBTITLE 1,Load Results DataFrame Rename Column and Show Count
results_df = spark.read \
.parquet(f"{processed_path}/results") \
.withColumnRenamed("time", "race_time")

display(results_df)
print(f"Number of Records Read {results_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### JOIN BETWEEN RACES & CIRCUITS

# COMMAND ----------

# DBTITLE 1,Join Races and Circuits DataFrames with Selected Column ...
join_races_circuits_df = races_df.join(circuit_df, races_df.circuit_id == circuit_df.circuit_id, "inner") \
.select("race_id", "race_year", "race_name", "date", "circuit_location")
display(join_races_circuits_df)

# COMMAND ----------

# DBTITLE 1,Join Race Results with Drivers Constructors and Circuit ...
from pyspark.sql.functions import current_timestamp, col

join_race_results_df = results_df.join(join_races_circuits_df, results_df.race_id == join_races_circuits_df.race_id, "inner") \
                                    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
                                    .join(constructors_df, results_df.constructor_id == constructors_df.cons_id, "inner") \
.select("race_year", "race_name", "date", "circuit_location", "driver_name", "driver_number", "driver_nationality", 
        "team", "grid", "fastest_lap", "race_time", "points", "position") \
.orderBy(col("points").desc()) \
.withColumn("created_dt", current_timestamp())

display(join_race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SAVE THE DATA IN THE PRESENTATION DATALAKE

# COMMAND ----------

# DBTITLE 1,Save Race Results DataFrame as Parquet with Overwrite M ...
join_race_results_df.write.mode("overwrite").parquet(f"{presentation_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SAVE THE DATA IN THE PRESENTATION DB

# COMMAND ----------

# DBTITLE 1,Save Race Results DataFrame to Parquet Table with Overw ...
join_race_results_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# DBTITLE 1,Load and Count Total Records in Race Results Table
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS cnt FROM f1_presentation.race_results

# COMMAND ----------

# DBTITLE 1,Display Total Record Count in Joined Race Results DataF ...
print(f"Number of Records {join_race_results_df.count()}")

# COMMAND ----------

# DBTITLE 1,Signal Notebook Completion with Success Message
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
