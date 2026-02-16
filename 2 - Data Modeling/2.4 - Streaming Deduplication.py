# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Here we apply __deduplication__ at the silver level as opposed to the bronze level. __Remember, the bronze table should retain history of the true state of the streaming data source__. This helps in the bigger picture of things by preventing potential for data loss due to applying aggressive quality enforcement and pre-processing during intial ingestion. Also, minimizes unnecessary latencies for data ingestion.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Before starting, we can identify the number of records in our orders topic of the bronze table. **For now simply using the read method, and not readStream method.**

# COMMAND ----------

(spark.read
      .table("bronze")
      .filter("topic = 'orders'")
      .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Given static data, we can simply use the dropDuplicates() function to eliminate duplicate records.

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

batch_total = (spark.read
                      .table("bronze")
                      .filter("topic = 'orders'")
                      .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                      .select("v.*")
                      .dropDuplicates(["order_id", "order_timestamp"])
                      .count()
                )

print(batch_total)

# COMMAND ----------

# MAGIC %md
# MAGIC In spark structured streaming, we can also use dropDuplicates() function. Structured streaming can track state information for the unique keys in the data. This ensures that duplicate records do not exist within or between microbatches. __Over time, this state information will scale to represent all history__. We can limit the amount of history to be maintained by using __Watermarking__.
# MAGIC
# MAGIC Watermarking allows to only track state information for a window of time in which we expect records.

# COMMAND ----------

deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'orders'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("order_timestamp", "30 seconds")
                   .dropDuplicates(["order_id", "order_timestamp"]))

# COMMAND ----------

# MAGIC %md
# MAGIC When dealing with streaming deduplication, there is another level of complexity compared to static data, as each micro-batch is processed. We can achieve this using insert-only merge. __This operation is ideal for deduplication__. It defines logic to match only unique keys, and only insert those records for keys that do not exist in the table.
# MAGIC
# MAGIC In the function below, we first store the records we inserted into a temporary view called orders_microbatch. We then use this temporary view in our insert-inly merge query. Last, we execute this query using a sparl SQL function.
# MAGIC
# MAGIC However, in this particular case, the spark session cannot be accessed from within the micro-batch process. Instead, we can access the local spark session from the microbatch dataframe.

# COMMAND ----------

def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch")
    
    sql_query = """
      MERGE INTO orders_silver a
      USING orders_microbatch b
      ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
      WHEN NOT MATCHED THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(sql_query)
    #microBatchDF._jdf.sparkSession().sql(sql_query) # below spark session 10.5, the syntax is slightly different.

# COMMAND ----------

# MAGIC %md
# MAGIC And since we are using SQL for writing to our Delta Table, we need to make sure that this table exists. We dropped the table in an earlier notebook...

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS orders_silver
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, in order to code the upsert function in our stream, we need to use the foreachBatch method. This provides the option to execute custom data writing logic on each micro batch of streaming data. In our case, the insert-only merge for deduplication.

# COMMAND ----------

query = (deduped_df.writeStream
                   .foreachBatch(upsert_data)
                   .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------

streaming_total = spark.read.table("orders_silver").count()

print(f"batch total: {batch_total}")
print(f"streaming total: {streaming_total}")

# COMMAND ----------

# MAGIC %md
# MAGIC The number of the unique records match the batch and streaming deduplication queries.

# COMMAND ----------


