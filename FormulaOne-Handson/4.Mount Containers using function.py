# Databricks notebook source
def mnt_adls(stg_accnt_nme,cntr_nme):
    client_id=''
    tenant_id=''
    client_secret=''

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    dbutils.fs.mount(
    source = f"abfss://{cntr_nme}@{stg_accnt_nme}.dfs.core.windows.net/",
    mount_point = f"/mnt/{stg_accnt_nme}/{cntr_nme}",
    extra_configs = configs)
    display(dbutils.fs.mounts())

# COMMAND ----------

## Mount containers
mnt_adls('gsdevadls1','presentation')

# COMMAND ----------

mnt_adls('gsdevadls1','presentation')
mnt_adls('gsdevadls1','processed')
