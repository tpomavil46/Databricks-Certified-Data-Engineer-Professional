# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Before starting, it is necessary to __cast the kafka binary fields as Strings__. This will allow us to actually view those binary fields in readable json format.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can __parse the orders data using the from_json function__. To achieve this, we must also provide the schema inscription.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC This logic can now be translated into a streaming read as follows. First, we must turn our static table into a streaming temporary view, allowing us to write streaming queries with sparkSQL.

# COMMAND ----------

(spark.readStream
      .table("bronze")
      .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can update the above query to reflect the streaming temporary view. __Such a streaming query will prevent from auto-terminating__.
# MAGIC
# MAGIC __Note:__ Do not forget to __terminate__ this if not using it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze_tmp
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we want to define this logic in a temporary view to pass it back to python. __Remember__ that a temporary view is used as an intermediary to capture the query we want to apply. This is how you can switch from SQL to Python.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC     FROM bronze_tmp
# MAGIC     WHERE topic = "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can create the orders_silver table. Simply use a streaming write to persist the result of the streaming temporary view to disk.
# MAGIC
# MAGIC Notice we use the triggerAvailableNow option, so all records will be processed in multiple microbatches until no more data is available and then stop the stream.

# COMMAND ----------

query = (spark.table("orders_silver_tmp")
               .writeStream
               .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
               .trigger(availableNow=True)
               .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Can we not express all of this using the pySpark API? Yes, we can!
# MAGIC
# MAGIC Here we can refactor to use Python syntax instead of SQL. We can use the filter function to retrieve the records of the orders topic, and the from_json function from the functions model in pySpark.

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC We can now query the orders_silver table and check the results.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC And now, our orders_silver table has been successfully generated from the multiplex bronze table.

# COMMAND ----------


