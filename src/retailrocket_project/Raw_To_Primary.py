# Databricks notebook source
bronze_stream = (
    spark.readStream
         .table("poc_prod.retail_raw.clickstream_events")
)

# COMMAND ----------

from pyspark.sql.types import *

event_schema = StructType([
    StructField("event_time", LongType()),
    StructField("visitor_id", LongType()),
    StructField("event_type", StringType()),
    StructField("item_id", LongType()),
    StructField("transaction_id", LongType()),
    StructField("source_system", StringType()),
    StructField("ingest_ts", StringType())
])

# COMMAND ----------

from pyspark.sql.functions import *

parsed_df = (
    bronze_stream
        .withColumn(
            "parsed",
            from_json(
                col("raw_json"),
                event_schema
            )
        )
)

# COMMAND ----------

flattened_df = (
    parsed_df
        .select(
            "partition",
            "offset",
            "eventhub_timestamp",
            "bronze_ingestion_ts",

            col("parsed.*")
        )
)

# COMMAND ----------

clean_df = (
    flattened_df
        .withColumn(
            "event_timestamp",
            to_timestamp(
                from_unixtime(
                    col("event_time") / 1000
                )
            )
        )
        .withColumn(
            "ingest_timestamp",
            to_timestamp("ingest_ts")
        )
)

# COMMAND ----------

quarantine_df = (
    clean_df
        .filter(
            col("visitor_id").isNull()
            |
            col("item_id").isNull()
            |
            col("event_timestamp").isNull()
        )
)

# COMMAND ----------

valid_df = (
    clean_df
        .filter(
            col("visitor_id").isNotNull()
            &
            col("item_id").isNotNull()
            &
            col("event_timestamp").isNotNull()
        )
)

# COMMAND ----------

watermarked_df = (
    valid_df
        .withWatermark(
            "event_timestamp",
            "10 minutes"
        )
)

# COMMAND ----------

dedup_df = (
    watermarked_df
        .dropDuplicates(
            [
                "partition",
                "offset"
            ]
        )
)

# COMMAND ----------

quarantine_checkpoint = (
    "abfss://data@eventhubdatasource.dfs.core.windows.net/"
    "checkpoints/silver/quarantine"
)
silver_checkpoint = (
    "abfss://data@eventhubdatasource.dfs.core.windows.net/"
    "checkpoints/silver/clickstream_clean"
)

# COMMAND ----------

(
    quarantine_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            quarantine_checkpoint
        )
        .trigger(availableNow=True)
        .toTable(
            "poc_prod.retail_primary.clickstream_quarantine"
        )
)

# COMMAND ----------

(
    dedup_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            silver_checkpoint
        )
        .trigger(availableNow=True)
        .toTable(
            "poc_prod.retail_primary.clickstream_clean"
        )
)