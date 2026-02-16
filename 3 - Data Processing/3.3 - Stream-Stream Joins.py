# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/customers_orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook, we will see how we can use CDF data to propagate changes to downstream table. For this demo, we will create a new silver table called customer_orders by joining two streams: The oders table with the CDF data of the customers_table.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC We start by creating a function to upsert ranked updates into our new table. We will use the same logic we used before for the CDC feed procesing in a previous demo. Here, in order to get those new changes we are filtering for two change types: insert and update_postimage. We use the commit timestamp column to sort the records in the window. Also notice we are using a composite key for partitioning the window and for merging our updates as well. Run the cell to create the function...

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())
    
    (microBatchDF.filter(F.col("_change_type").isin(["insert", "update_postimage"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank = 1")
                 .drop("rank", "_change_type", "_commit_version")
                 .withColumnRenamed("_commit_timestamp", "processed_timestamp")
                 .createOrReplaceTempView("ranked_updates"))
    
    query = """
        MERGE INTO customers_orders c
        USING ranked_updates r
        ON c.order_id=r.order_id AND c.customer_id=r.customer_id
            WHEN MATCHED AND c.processed_timestamp < r.processed_timestamp
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we define our new table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_orders
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP, processed_timestamp TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can define a streaming query to write our new table customers_orders. Here we are performing a join operation between two streaming tables. We start by reading the orders tableas a streaming source, and reading customers data as a streaming source. Then, we are performing an inner jpoin between these two data frames based on the customer_id key.
# MAGIC
# MAGIC When performing stream-stream joins, Saprk buffers past input as streaming state for both input streams, so that it can match every future input with past imputs and accordingly generate the joined results (Spark will automatically perform micro-batching).
# MAGIC
# MAGIC And of course, similar to the streaming duplication we saw previously, we can limit the state using watermarks. Run the below cell to process the records using the foreachBatch logic.

# COMMAND ----------

def process_customers_orders():
    orders_df = spark.readStream.table("orders_silver")
    
    cdf_customers_df = (spark.readStream
                             .option("readChangeData", True)
                             .option("startingVersion", 2)
                             .table("customers_silver")
                       )

    query = (orders_df
                .join(cdf_customers_df, ["customer_id"], "inner")
                .writeStream
                    .foreachBatch(batch_upsert)
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_orders")
                    .trigger(availableNow=True)
                    .start()
            )
    
    query.awaitTermination()
    
process_customers_orders()

# COMMAND ----------

# MAGIC %md
# MAGIC Review the data in the new table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_orders

# COMMAND ----------

# MAGIC %md
# MAGIC We can now land a new data file to see how changes will be propagated through our pipeline.

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

process_customers_orders()

# COMMAND ----------

# MAGIC %md
# MAGIC New updates have been propagated and merged into the new table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customers_orders

# COMMAND ----------


