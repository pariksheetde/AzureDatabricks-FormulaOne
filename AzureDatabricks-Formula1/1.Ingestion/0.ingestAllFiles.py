# Databricks notebook source
dbutils.notebook.run("1.ingestCircuitsFile", 600)

# COMMAND ----------

dbutils.notebook.run("2.ingestRacesFile", 600)

# COMMAND ----------

dbutils.notebook.run("3.ingestConstructorsFile", 600)

# COMMAND ----------

dbutils.notebook.run("4.ingestDriversFile", 600)

# COMMAND ----------

dbutils.notebook.run("5.ingestResultsFile", 1200)

# COMMAND ----------

dbutils.notebook.run("6.ingestPitstopsFile", 600)

# COMMAND ----------

dbutils.notebook.run("7.ingestLapTimesFile", 600)

# COMMAND ----------

dbutils.notebook.run("8.ingestQualifyingFile", 600)
