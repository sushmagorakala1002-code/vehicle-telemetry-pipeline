# Databricks notebook source
client_id = dbutils.secrets.get(scope ="just_practice" , key ="client-id")
tenant_id = dbutils.secrets.get(scope ="just_practice" , key ="tenant-id")
client_secret = dbutils.secrets.get(scope ="just_practice" , key ="client-secret")
storage = "justpracticeadls"


# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
