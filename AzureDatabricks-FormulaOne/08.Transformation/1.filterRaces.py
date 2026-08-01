# Databricks notebook source
# DBTITLE 1,Load Configuration Settings from External File
# MAGIC %run "../09.Includes/1.config"

# COMMAND ----------

# DBTITLE 1,Load and Inspect Races Data from Processed Path
dl_races_df = spark.read \
.parquet(f"{processed_path}/races")

# display(dl_races_df)
dl_races_df.printSchema()
print(f"Number of Records Read {dl_races_df.count()}")
print(processed_path)

# COMMAND ----------

# DBTITLE 1,Filter Australian Races Data for Year 2019
from pyspark.sql.functions import col

filtered_races_df = dl_races_df.filter("race_year = 2019 and name like 'Australian%'")
# display(filtered_races_df)

# COMMAND ----------

# DBTITLE 1,Filter Non-Australian Races Data for Year 2019
from pyspark.sql.functions import col

filtered_races_df = dl_races_df.filter((dl_races_df["race_year"] == 2019) & (dl_races_df["name"] != "Australian Grand Prix"))
# display(filtered_races_df)

# COMMAND ----------

# DBTITLE 1,Confirm Notebook Completion with Success Status
dbutils.notebook.exit("EXECUTED SUCCESSFULLY")
