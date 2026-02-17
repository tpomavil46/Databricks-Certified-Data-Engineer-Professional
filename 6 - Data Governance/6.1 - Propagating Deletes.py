# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/deletes.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook demostrates how to incrementally process delete requests and propagate deletes. And how to propagate deletes through the lakehouse. Delete requests are also known as requests to be forgotten. They require deleteing user data that represent Personally Identifiable Information (PII), such as name and email of the user. We have two tables here that contain PII data, which are the customers and customers_orders silver tables.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Here we set up a table called delete_requests to track users requests to be forgotten. These requests are received in the bronze table as CDC type "delete" in the customers topic. While it is possible to process deletes at the same time as inserts and updates CDC feed, the fines around right to be forgotten requets may require a separate process.

# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

(spark.readStream
        .table("bronze")
        .filter("topic = 'customers'")
        .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
        .select("v.*", F.col('v.row_time').alias("request_timestamp")) # here, we indicate the time at which the delete was # requested
        .filter("row_status = 'delete'")
        .select("customer_id", "request_timestamp",
                F.date_add("request_timestamp", 30).alias("deadline"), # and we add 30 days as a deadline to ensure 
                # compliance 
                F.lit("requested").alias("status")) # we provide a field that indicates the current process of the delete 
                # request
    .writeStream
        .outputMode("append")
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/delete_requests")
        .trigger(availableNow=True)
        .table("delete_requests")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Review the delete requests table. We should see 126 delete requests.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC Now, we can start deleting user records from the customers table based on the customer ID.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM customers_silver
# MAGIC WHERE customer_id IN (SELECT customer_id FROM delete_requests WHERE status = 'requested')

# COMMAND ----------

# MAGIC %md
# MAGIC In a previous notebook, CDF was enabled on the customers table. So we can now leverage CDF data as an incremental records of data changes to propagate deletes to downstream tables. Here we can figure an incremental read of all change events committed to the customers table.

# COMMAND ----------

deleteDF = (spark.readStream
                 .format("delta")
                 .option("readChangeFeed", "true")
                 .option("startingVersion", 2)
                 .table("customers_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can define a function to be called with foreachBatch in order to process the delete events. Here we commit deletes to other tables containing PII data in our pipeline. Once finished, we perform an update back to the delete_requests table to change the status of the request to deleted.

# COMMAND ----------

def process_deletes(microBatchDF, batchId):
    
    (microBatchDF
        .filter("_change_type = 'delete'")
        .createOrReplaceTempView("deletes"))

    microBatchDF._jdf.sparkSession().sql("""
        DELETE FROM customers_orders
        WHERE customer_id IN (SELECT customer_id FROM deletes)
    """)
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO delete_requests r
        USING deletes d
        ON d.customer_id = r.customer_id
        WHEN MATCHED
          THEN UPDATE SET status = "deleted"
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC Last, we can run a trigger availableNow batch to propagate deletes using our foreachBatch logic.

# COMMAND ----------

(deleteDF.writeStream
         .foreachBatch(process_deletes)
         .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/deletes")
         .trigger(availableNow=True)
         .start())

# COMMAND ----------

# MAGIC %md
# MAGIC Review the delete commits

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC Also, review what happened in the customers_orders table. As expected, the log shows a delete operation performed on the table. But the question now, are deletes fully committed? No, not exactly! Because of the way Delta Lake tables time travel and CDF features are implemented, deleted values are still present in older versions of the data. Recall that with Delta Lake, deleting data does not delete the data files from the table directory. Instead, it creates a copy of the affected files without these deleted records. Confirm this...

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_orders

# COMMAND ----------

# MAGIC %md
# MAGIC This query will show only the records deleted in the previous version of the customer_orders table. And sure enough, the deleted records are still there...

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM customers_orders@v6
# MAGIC -- EXCEPT
# MAGIC SELECT * FROM customers_orders

# COMMAND ----------

# MAGIC %md
# MAGIC Similarly, while deletes already committed to the customers table, info is still available within the CDF feed. In short, to fully delete these commits, vaccuum commands must be run on these tables.

# COMMAND ----------

df = (spark.read
           .option("readChangeFeed", "true")
           .option("startingVersion", 2)
           .table("customers_silver")
           .filter("_change_type = 'delete'"))
display(df)

# COMMAND ----------


