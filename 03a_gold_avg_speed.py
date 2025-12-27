# Databricks notebook source
"""
Gold layer:
- Aggregates Silver telemetry using event-time windowing
- Computes vehicle-level metrics avg speed
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

gold_speed_path = f"abfss://gold@{storage}.dfs.core.windows.net/vehicle_speed_5min"
chk_speed_5min = f"abfss://checkpoint-gold@{storage}.dfs.core.windows.net/chk_vehicle_speed_5min"

# COMMAND ----------

from pyspark.sql.functions import window, avg, col

# COMMAND ----------

vehicle_speed_5min_df = (
    silver_df
    .withColumn("event_time", col("event_time").cast("timestamp"))
    .withWatermark("event_time", "10 minutes") 
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("vehicle_id")
    )
    .agg(
        avg(col("speed_kmph")).alias("avg_speed")
    )
    .select(
        col("vehicle_id"),
        col("avg_speed"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end")
    )
)


# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------


def merge_speed_gold(batch_df, batch_id):
    delta_table = DeltaTable.forPath(spark, gold_speed_path)

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
            "avg_speed": "s.avg_speed"
        })
        .whenNotMatchedInsert(values={
            "vehicle_id": "s.vehicle_id",
            "avg_speed": "s.avg_speed",
            "window_start": "s.window_start",
            "window_end": "s.window_end"
        })
        .execute()
    )


# COMMAND ----------

vehicle_speed_5min_df.writeStream \
  .outputMode("update") \
  .foreachBatch(merge_speed_gold) \
  .option("checkpointLocation",chk_speed_5min ) \
  .start()
