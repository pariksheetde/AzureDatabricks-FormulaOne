# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External Script
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# DBTITLE 1,Load and Display Race Results Dataframe from Parquet
race_results_df = spark.read.parquet(f"{presentation_path}/race_results")
display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### CONSTRUCTORS'S STANDING

# COMMAND ----------

# DBTITLE 1,Aggregate team points and wins by race year
from pyspark.sql.functions import *

constructors_standing_df = race_results_df.groupBy("race_year", "team") \
.agg(
    sum("points").alias("sum_points"),
    count(when(col("position") == 1, True)).alias("wins")
   )
# display(constructors_standing_df)

# COMMAND ----------

# DBTITLE 1,Rank Constructors by Points and Wins for 2020 Season
from pyspark.sql.functions import *
from pyspark.sql.window import Window

windowSpec = Window \
    .partitionBy("race_year") \
    .orderBy(col("sum_points").desc(), col("wins").desc())

constructors_rank_spec_df = constructors_standing_df.select("race_year", "team", "sum_points", "wins") \
.filter("race_year = 2020") \
.withColumn("rank", rank().over(windowSpec))

# display(constructors_rank_spec_df)

# COMMAND ----------

# DBTITLE 1,Save Constructors Ranking Dataframe as Parquet File
constructors_rank_spec_df.write.mode("overwrite").parquet(f"{presentation_path}/constructors_standing")

# COMMAND ----------

# DBTITLE 1,Display Count of Records in Constructors Rank Dataframe
print(f"Number of Records Effected: {constructors_rank_spec_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SAVE THE DATA IN THE PRESENTATION DB

# COMMAND ----------

# DBTITLE 1,Save Constructors Standings Dataframe to Presentation T ...
constructors_rank_spec_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructors_standing")

# COMMAND ----------

# DBTITLE 1,Display Total Number of Records in Constructors Standin ...
# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_presentation.constructors_standing;

# COMMAND ----------

# DBTITLE 1,Finalize Notebook Execution with Success Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
