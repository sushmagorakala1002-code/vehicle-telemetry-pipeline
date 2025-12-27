# Databricks notebook source
"""
Gold layer:
- Aggregates Silver telemetry using event-time windowing
- Computes vehicle-level metrics  max engine temp.
- Uses watermarking for late data handling
- Writes analytics-ready Delta tables for BI and reporting
"""

# COMMAND ----------

# MAGIC %run ./adls_config

# COMMAND ----------

storage = "justpracticeadls"

silver_path = f"abfss://silver@{storage}.dfs.core.windows.net/vehicle_telemetry"

# COMMAND ----------

silver_df = spark.readStream.format("delta").load(silver_path)

# COMMAND ----------

gold_engine_path = f"abfss://gold@{storage}.dfs.core.windows.net/vehicle_engine_temp_5min"
chk_engine_temp = f"abfss://checkpoint-gold@{storage}.dfs.core.windows.net/chk_vehicle_engine_temp_5min"

# COMMAND ----------

from pyspark.sql.functions import window,col,max

# COMMAND ----------

vehicle_engine_temp_5min_df = (
    silver_df
    .withColumn("event_time", col("event_time").cast("timestamp"))
    .withWatermark("event_time", "10 minutes") 
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("vehicle_id")
    )
    .agg(
        max(col("engine_temp_c")).alias("max_engine_temp")
    )
    .select(
        col("vehicle_id"),
        col("max_engine_temp"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end")
    )
)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

def merge_engine_gold(batch_df, batch_id):
    delta_table = DeltaTable.forPath(spark, gold_engine_path)

    (
        delta_table.alias("t")
        .merge(
            batch_df.alias("s"),
            """
            t.vehicle_id = s.vehicle_id AND
            t.window_start = s.window_start AND
            t.window_end = s.window_end
            """
        )
        .whenMatchedUpdate(set={
            "max_engine_temp": "s.max_engine_temp"
        })
        .whenNotMatchedInsert(values={
            "vehicle_id": "s.vehicle_id",
            "max_engine_temp": "s.max_engine_temp",
            "window_start": "s.window_start",
            "window_end": "s.window_end"
        })
        .execute()
    )


# COMMAND ----------

vehicle_engine_temp_5min_df.writeStream \
  .outputMode("update") \
  .foreachBatch(merge_engine_gold) \
  .option("checkpointLocation", chk_engine_temp) \
  .start()