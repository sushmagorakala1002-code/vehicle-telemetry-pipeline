# Databricks notebook source
"""
Bronze layer:
- Ingests raw vehicle telemetry from Event Hubs
- Applies explicit schema
- Adds ingestion metadata
- Writes raw events to Delta with exactly-once guarantees
"""

# COMMAND ----------

# MAGIC %run ./adls_config

# COMMAND ----------

conn = dbutils.secrets.get(scope ="just_practice" , key ="connection-string").strip()

# COMMAND ----------

ehConf = {
  "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn),
  "eventhubs.consumerGroup": "$Default"
}

# COMMAND ----------

raw_df = (
  spark.readStream
  .format("eventhubs")
  .options(**ehConf)
  .load()
)

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType

# COMMAND ----------

schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("producer_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("speed_kmph", IntegerType()),
    StructField("engine_temp_c", IntegerType()),
    StructField("fuel_level_pct", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType())
])

# COMMAND ----------

parsed_df = (
  raw_df
  .select(from_json(col("body").cast("string"), schema).alias("data"))
  .select("data.*")
)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
bronze_df = (
  parsed_df
  .withColumn("ingestion_time", current_timestamp())
)

# COMMAND ----------

storage = "justpracticeadls"

bronze_path = f"abfss://bronze@{storage}.dfs.core.windows.net/vehicle_telemetry"
check_path  = f"abfss://checkpoint-bronze@{storage}.dfs.core.windows.net/bronze_vehicle_telemetry"

# COMMAND ----------

query = (
  bronze_df
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("path", bronze_path)
  .option("checkpointLocation", check_path)
  .option("failOnDataLoss", "false")
  .start()
)