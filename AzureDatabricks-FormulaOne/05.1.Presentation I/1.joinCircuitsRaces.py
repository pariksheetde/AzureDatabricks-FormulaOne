# Databricks notebook source
# DBTITLE 1,Load Configuration Settings for Notebook Execution
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ circuits.parquet FROM DATALAKE (PROCESSED DATA)

# COMMAND ----------

# DBTITLE 1,Load Circuits Data and Print Record Count
dl_circuits_df = spark.read \
.parquet(f"{processed_path}/circuits") \
.withColumnRenamed("name", "circuit_name")

# display(dl_circuits_df)
print(f"Number of Records Read {dl_circuits_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ races.parquet FROM DATALAKE (PROCESSED DATA)

# COMMAND ----------

# DBTITLE 1,Load 2019 Races Data and Display Record Count
dl_races_df = spark.read \
.parquet(f"{processed_path}/races").filter("race_year = 2019") \
.withColumnRenamed("name", "race_name")

# display(dl_races_df)
print(f"Number of Records Read {dl_races_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### INNER JOIN

# COMMAND ----------

# DBTITLE 1,Join Circuits and Races Data and Select Key Details
race_circuit_df = dl_circuits_df.join(dl_races_df, dl_circuits_df.circuit_id == dl_races_df.circuit_id, "inner") \
.select("circuit_name", "location", "country", "race_name" ,"round")

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SEMI JOIN

# COMMAND ----------

# DBTITLE 1,Join Circuits and Races Data to Extract Key Location In ...
race_circuit_df = dl_circuits_df.join(dl_races_df, dl_circuits_df.circuit_id == dl_races_df.circuit_id, "semi") \
.select("circuit_name", "circuit_ref", "country", "location")

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ANTI JOIN

# COMMAND ----------

# DBTITLE 1,Find Circuits Without Corresponding Races and Show Deta ...
race_circuit_df = dl_circuits_df.join(dl_races_df, dl_circuits_df.circuit_id == dl_races_df.circuit_id, "anti") \
.select("circuit_name", "circuit_ref", "country", "location")

display(race_circuit_df)

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Successful Execution Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
