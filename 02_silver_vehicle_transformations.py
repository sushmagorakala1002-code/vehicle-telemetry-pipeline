# Databricks notebook source
"""
Silver layer:
- Reads streaming telemetry from Bronze Delta tables
- Enforces data types and drops malformed records
- Applies business rule validations (speed, engine temp, geo)
- Deduplicates events using event-time watermarking
- Derives analytical fields (speed_mps, flags, ingestion delay)
- Writes curated, analytics-ready data to Silver Delta tables
"""

# COMMAND ----------

# MAGIC %run ./adls_config
# MAGIC

# COMMAND ----------

storage = "justpracticeadls"

bronze_path = f"abfss://bronze@{storage}.dfs.core.windows.net/vehicle_telemetry"

# COMMAND ----------

bronze_df = (
    spark.readStream
         .format("delta")
         .load(bronze_path)
)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cast the columns

# COMMAND ----------

bronze_drop_null_df = bronze_df.select(
    "*",
).dropna(subset=["vehicle_id", "event_time", "speed_kmph", "engine_temp_c"])

# COMMAND ----------

bronze_cast_df = (bronze_drop_null_df
                  .withColumn("speed_kmph",col("speed_kmph").cast(DoubleType())) 
                  .withColumn("engine_temp_c",col("engine_temp_c").cast(DoubleType()))
                  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter the columns

# COMMAND ----------

bronze_filter1_df = bronze_cast_df.filter(
    (col("speed_kmph") > 0) & (col("speed_kmph") <= 250)
)



# COMMAND ----------

bronze_filter2_df = bronze_filter1_df.filter(
                    (col("engine_temp_c") >= 60) & (col("engine_temp_c") <= 120)
                    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop duplicates

# COMMAND ----------

bronze_duplicates_df = bronze_filter2_df.withWatermark("event_time", "10 minutes").dropDuplicates(["vehicle_id", "event_time"])



# COMMAND ----------

# MAGIC %md
# MAGIC ### Derived Columns

# COMMAND ----------

from pyspark.sql.functions import when,col, round

# COMMAND ----------

bronze_der1_df= bronze_duplicates_df.withColumn("is_engine_hot", col("engine_temp_c") >= 100)

# COMMAND ----------

bronze_der2_df = bronze_der1_df.withColumn("speed_mps", round(col("speed_kmph") * 0.277778,2))

# COMMAND ----------

bronze_der3_df = bronze_der2_df.withColumn("is_vehicle_moving", col("speed_kmph") > 5)

# COMMAND ----------

bronze_der4_df = bronze_der3_df.withColumn("ingestion_delay_sec", 
                                           (col("ingestion_time").cast("long") - col("event_time").cast("long")))

# COMMAND ----------

bronze_der5_df = bronze_der4_df.withColumn("is_geo_valid", 
                                           (col("latitude").between(-90,90)) & (col("longitude").between(-180,180)))

# COMMAND ----------

silver_df = bronze_der5_df.select(
    "vehicle_id",
    "producer_id",
    "event_time",
    "speed_kmph",
    "speed_mps",
    "is_vehicle_moving",
    "engine_temp_c",
    "is_engine_hot",
    "fuel_level_pct",
    "latitude",
    "longitude",
    "is_geo_valid",
    "ingestion_time",
    "ingestion_delay_sec"
)


# COMMAND ----------

silver_path = f"abfss://silver@{storage}.dfs.core.windows.net/vehicle_telemetry"
check_silver_path  = f"abfss://checkpoint-silver@{storage}.dfs.core.windows.net/silver_vehicle_telemetry"

# COMMAND ----------

query = (
    silver_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("path", silver_path)
    .option("checkpointLocation", check_silver_path)
    .option("queryName", "silver_vehicle_stream")
    .start()
)
