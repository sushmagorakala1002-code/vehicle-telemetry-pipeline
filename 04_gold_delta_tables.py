# Databricks notebook source
# MAGIC %run ./adls_config

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.vehicle_speed_5min;
# MAGIC DROP TABLE IF EXISTS default.vehicle_engine_temp_5min;
# MAGIC DROP TABLE IF EXISTS default.vehicle_event_count_5min;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.vehicle_speed_5min
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@justpracticeadls.dfs.core.windows.net/vehicle_speed_5min';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.vehicle_engine_temp_5min
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@justpracticeadls.dfs.core.windows.net/vehicle_engine_temp_5min';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.vehicle_event_count_5min
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@justpracticeadls.dfs.core.windows.net/vehicle_event_count_5min';