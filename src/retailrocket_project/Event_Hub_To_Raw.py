# Databricks notebook source
event_hub_namespace = "rr-clickstream-ns"
event_hub_name = "retailrocket-events"

eventhub_connection_string = dbutils.secrets.get(
    scope="retailrocket",
    key="eventhub-connection-string"
)

bootstrap_servers = (
    f"{event_hub_namespace}.servicebus.windows.net:9093"
)

# COMMAND ----------

kafka_options = {
    "kafka.bootstrap.servers":
        "rr-clickstream-ns.servicebus.windows.net:9093",

    "subscribe":
        "retailrocket-events",

    "kafka.security.protocol":
        "SASL_SSL",

    "kafka.sasl.mechanism":
        "PLAIN",

    "kafka.sasl.jaas.config":
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" '
        f'password="{eventhub_connection_string}";',

    "startingOffsets":
        "earliest",

    "failOnDataLoss":
        "false"
}

# COMMAND ----------

raw_stream = (
    spark.readStream
         .format("kafka")
         .options(**kafka_options)
         .load()
)

# COMMAND ----------

raw_stream.printSchema()

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    current_timestamp
)

bronze_df = (
    raw_stream
        .select(
            col("key").cast("string").alias("event_key"),

            col("value")
                .cast("string")
                .alias("raw_json"),

            col("topic"),

            col("partition"),

            col("offset"),

            col("timestamp")
                .alias("eventhub_timestamp"),

            current_timestamp()
                .alias("bronze_ingestion_ts")
        )
)

# COMMAND ----------

checkpoint_path = (
    "abfss://data@eventhubdatasource.dfs.core.windows.net/"
    "checkpoint/bronze/clickstream_events"
)

# COMMAND ----------

(
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable("poc_prod.retail_raw.clickstream_events")
)

# COMMAND ----------

query.status