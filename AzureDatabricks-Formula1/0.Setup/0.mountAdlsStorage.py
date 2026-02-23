# Databricks notebook source
# MAGIC %md
# MAGIC #### ACCESS ADLS USING SERVICE PRINCIPAL
# MAGIC 1. client_id
# MAGIC 2. secret_id
# MAGIC 3. tenant_id

# COMMAND ----------

# DBTITLE 1,Set Azure Storage Account Credentials for Authenticatio ...
storage_account_name = "formula1dbdevadls"
client_id = "563125fa-2081-48bb-8403-118ac72eed6d"
client_secret = "kGX8Q~t6Cb4Qh0P.Y82rDQ9kC5iRokXO7GKqMcq~"
tenant_id = "9cd5292d-d337-4834-b68a-15f1ebfcf00c"

# COMMAND ----------

# DBTITLE 1,Configure OAuth Credentials for Azure Storage Access
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a UDF to mount the container in adls

# COMMAND ----------

# DBTITLE 1,Define Function to Mount Azure Data Lake Storage Contai ...
def mount_adls(container_name):
  storage_name = "formula1dbdevadls"
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### MOUNT RAW CONTAINER

# COMMAND ----------

# DBTITLE 1,Mount Azure Data Lake Storage and List Directory Conten ...
try:
    dbutils.fs.unmount("/mnt/formula1dbdevadls/raw")
except Exception:
    pass
mount_adls("raw")
dbutils.fs.ls("/mnt/formula1dbdevadls/raw")

# COMMAND ----------

# MAGIC %md
# MAGIC #### MOUNT PROCESSED CONTAINER

# COMMAND ----------

# DBTITLE 1,Unmount and Remount ADLS Processed Container with Direc ...
try:
    dbutils.fs.unmount("/mnt/formula1dbdevadls/processed")
except Exception:
    pass
mount_adls("processed")
dbutils.fs.ls("/mnt/formula1dbdevadls/processed")

# COMMAND ----------

# MAGIC %md
# MAGIC #### MOUNT PRESENTATION CONTAINER
# MAGIC

# COMMAND ----------

try:
    dbutils.fs.unmount("/mnt/formula1dbdevadls/presentation")
except Exception:
    pass
mount_adls("presentation")
dbutils.fs.ls("/mnt/formula1dbdevadls/presentation")

# COMMAND ----------

# MAGIC %md
# MAGIC #### MOUNT INCREMENTAL CONTAINER

# COMMAND ----------

try:
    dbutils.fs.unmount("/mnt/formula1dbdevadls/incremental")
except Exception:
    pass
mount_adls("incremental")
dbutils.fs.ls("/mnt/formula1dbdevadls/incremental")

# COMMAND ----------

# MAGIC %md
# MAGIC #### MOUNT DELTALAKE CONTAINER
# MAGIC

# COMMAND ----------

try:
    dbutils.fs.unmount("/mnt/formula1dbdevadls/deltalake")
except Exception:
    pass
mount_adls("deltalake")
dbutils.fs.ls("/mnt/formula1dbdevadls/deltalake")

# COMMAND ----------

dbutils.notebook.exit("MOUNTING OF ADLS COMPLETED SUCCESSFULLY")
