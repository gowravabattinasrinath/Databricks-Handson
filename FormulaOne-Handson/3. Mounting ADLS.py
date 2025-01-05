# Databricks notebook source
display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

client_id=''
tenant_id=''
client_secret=''

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.gsdevadls1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.gsdevadls1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.gsdevadls1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.gsdevadls1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.gsdevadls1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@gsdevadls1.dfs.core.windows.net/",
  mount_point = "/mnt/gsdevadls1/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/gsdevadls1/demo'))

# COMMAND ----------

display(spark.read.csv("/mnt/gsdevadls1/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

##dbutils.fs.unmount("/mnt/gsdevadls1/demo")
