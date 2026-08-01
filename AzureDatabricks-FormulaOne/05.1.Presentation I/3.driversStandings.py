# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Script
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# DBTITLE 1,Load and Display Race Results Data from Parquet
race_results_df = spark.read.parquet(f"{presentation_path}/race_results")
# display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DRIVERS'S STANDING

# COMMAND ----------

# DBTITLE 1,Aggregate Driver Points and Wins by Year and Team
from pyspark.sql.functions import *

drivers_standing_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(
    sum("points").alias("sum_points"),
    count(when(col("position") == 1, True)).alias("wins")
   )
# display(drivers_standing_df)

# COMMAND ----------

# DBTITLE 1,Compute and Rank Driver Standings for 2020 Season
from pyspark.sql.functions import *
from pyspark.sql.window import Window

windowSpec = Window \
    .partitionBy("race_year") \
    .orderBy(col("sum_points").desc(), col("wins").desc())

drivers_rank_spec_df = drivers_standing_df.select("race_year", "driver_name", "driver_nationality", "team", "sum_points", "wins") \
.filter("race_year = 2020") \
.withColumn("rank", rank().over(windowSpec))

# display(drivers_rank_spec_df)

# COMMAND ----------

# DBTITLE 1,Save Drivers Ranking Dataframe as Parquet File
drivers_rank_spec_df.write.mode("overwrite").parquet(f"{presentation_path}/drivers_standing")

# COMMAND ----------

# DBTITLE 1,Display Total Number of Rows in Drivers Ranking DataFra ...
print(f"Number of Rows Effected {drivers_rank_spec_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SAVE THE DATA IN THE PRESENTATION DB

# COMMAND ----------

# DBTITLE 1,Persist Drivers Standings Dataframe to Managed Table
drivers_rank_spec_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.drivers_standings")

# COMMAND ----------

# DBTITLE 1,Display Total Count of Records in Drivers Standings Tab ...
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.drivers_standings;

# COMMAND ----------

# DBTITLE 1,Exit Notebook with Successful Execution Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
