# Databricks notebook source
# MAGIC %run ./adls_config

# COMMAND ----------

# MAGIC %sql
DROP TABLE IF EXISTS default.vehicle_speed_5min;
DROP TABLE IF EXISTS default.vehicle_engine_temp_5min;
DROP TABLE IF EXISTS default.vehicle_event_count_5min;

# COMMAND ----------

# MAGIC %sql
CREATE TABLE IF NOT EXISTS default.vehicle_speed_5min
USING DELTA
LOCATION 'abfss://gold@justpracticeadls.dfs.core.windows.net/vehicle_speed_5min';

# COMMAND ----------

# MAGIC %sql
CREATE TABLE IF NOT EXISTS default.vehicle_engine_temp_5min
USING DELTA
LOCATION 'abfss://gold@justpracticeadls.dfs.core.windows.net/vehicle_engine_temp_5min';

# COMMAND ----------

# MAGIC %sql
CREATE TABLE IF NOT EXISTS default.vehicle_event_count_5min
USING DELTA
LOCATION 'abfss://gold@justpracticeadls.dfs.core.windows.net/vehicle_event_count_5min';
