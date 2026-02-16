# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/customers.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook, I am processing Change Data Capture (CDC). We'll create a __customers_silver__ table. The data in the customers topic contains complete row output from the Change Data Capture feed. The changes captured are either insert, update, or delete.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC We take a look at the customers data first. We filter, as before, on the customers data topic, upack all the json fields from the value column into the cirrect schema. In addition, we will process only insert and update in this notebook. We will see different approaches for processing delete requests in another lecture.

# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

customers_df = (spark.table("bronze")
                 .filter("topic = 'customers'")
                 .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                 .select("v.*")
                 .filter(F.col("row_status").isin(["insert", "update"])))

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Above, we see the customer ID, email, first and last name, gender, and the address information for each customer, street, city, and country code. In addition we have the cdc fields: row status and row time. As can be seen, both insert and update records contain all the fields we need for the customers table. Multiple updates could be recieved for the same record, but with different row times. We need to select the most recent record for each customer. We previously saw the dropDuplicates function to drop exact duplicates. However, here the problem is different since the records are not identical for the primary key.

# COMMAND ----------

# MAGIC %md
# MAGIC The soltuion to keep only the most recent record is to use the rank window function. The ranking function assigns a rank number for each row within a window. A window is a group of ordered records having the same partition key, in this case, customer ID. And we sort the records within our window by the row time in descending order such that the most recent record for each customer ID will have a rank of 1. We can then filter to keep oonly records have a rank of 1 and drop the rank column as it is no longer needed.

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())

ranked_df = (customers_df.withColumn("rank", F.rank().over(window))
                          .filter("rank == 1")
                          .drop("rank"))
display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, we can apply this same logic to a streaming read of the `customers` topic. But, such a window operation is not supported on streaming dataframes.

# COMMAND ----------

# This will throw an exception because non-time-based window operations are not supported on streaming DataFrames.
ranked_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'customers'")
                   .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                   .select("v.*")
                   .filter(F.col("row_status").isin(["insert", "update"]))
                   .withColumn("rank", F.rank().over(window))
                   .filter("rank == 1")
                   .drop("rank")
             )

display(ranked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC To avoid the above restriction, we can use the forEachBatch logic. Inside the streaming micro-batch process, we can interact with the data using batch syntax instead of streaming syntax. Thw idea here is to simply process the records of each batch before merging them into the target table. 
# MAGIC
# MAGIC We start by computing the newest entries based on the window, and store them in a temporary view called ranked_updates. We can then merge the ranked updates into the customer table based on the customer ID key. If the key already exists in the table, we update the record. If the key does not exist, we insert a new record.
# MAGIC
# MAGIC If we were inerested in applying delete changes as well, we could simply add another condition for this in our merge statement.
# MAGIC
# MAGIC WHEN MATCHED AND r.rowstatus = 'delete'
# MAGIC               THEN DELETE

# COMMAND ----------

from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    
    (microBatchDF.filter(F.col("row_status").isin(["insert", "update"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank == 1")
                 .drop("rank")
                 .createOrReplaceTempView("ranked_updates"))
    
    query = """
        MERGE INTO customers_silver c
        USING ranked_updates r
        ON c.customer_id=r.customer_id
            WHEN MATCHED AND c.row_time < r.row_time
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_silver
# MAGIC (customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice above we are adding country name instead of the country code. For this, we will perform a join with a separate country lookup table.

# COMMAND ----------

df_country_lookup = spark.read.json(f"{dataset_bookstore}/country_lookup")
display(df_country_lookup)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can effectively write the desired streaming query. What we are doing is enriching our customer data by performing the join with the country lookup table. Notice also that we are using a broadcast join with this small lookup table to avoid shuffling the data.
# MAGIC
# MAGIC This is an optimization technique that is available in Spark where the smaller dataframe will be sent to all the executor nodes in the cluster. This is useful when the smaller dataframe is expected to be joined with the larger dataframe multiple times.
# MAGIC
# MAGIC To allow for this, you just need to mark which dataframe is small enough for broadcasting using the broadcast() function. This hints to Spark that this dataframe can fit in the memory on all executors.
# MAGIC
# MAGIC Next, we use foreachBatch to merge the newest changes. And last, running with the available now trigger to process all the records.

# COMMAND ----------

query = (spark.readStream
                  .table("bronze")
                  .filter("topic = 'customers'")
                  .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                  .select("v.*")
                  .join(F.broadcast(df_country_lookup), F.col("country_code") == F.col("code") , "inner")
               .writeStream
                  .foreachBatch(batch_upsert)
                  .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_silver")
                  .trigger(availableNow=True)
                  .start()
          )

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC The customers table should now have only one record for each unique ID. Confirm this below with a unit test. We use an assert statement to confirm this. 
# MAGIC
# MAGIC Note: Assertions are boolean expressions that check if a statement is true or false. The idea is to use them in unit tests to check if certain assumptions remain true while developing.

# COMMAND ----------

count = spark.table("customers_silver").count()
expected_count = spark.table("customers_silver").select("customer_id").distinct().count()

assert count == expected_count
print("Unit test passed.")

# COMMAND ----------


