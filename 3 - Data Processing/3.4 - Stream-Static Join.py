# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/books_sales.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook, we see how to join a stream with a static dataset. We will create the silver table books_sales by joining the orders streaming table with the current books (static table). Here the current_books table is no longer a streamable. Remember this table is updated using batch overwrite logic so it breaks the requirement of an ever appending source for structured streaming.
# MAGIC
# MAGIC Fortunately, Delta Lake guarantees that the latest version of a static table is returned each time it is queried in a streaming static join.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we read our orders table a a streaming source using spark.readStream while we read the current_books table as a static source using spark.read.table. Now we can simply join the two dataframes as usual. Last, we are using the trigger option available in spark.readStream to process the data as soon as it arrives in the append mode.

# COMMAND ----------

from pyspark.sql import functions as F

def process_books_sales():
    
    orders_df = (spark.readStream.table("orders_silver")
                        .withColumn("book", F.explode("books"))
                )

    books_df = spark.read.table("current_books")

    query = (orders_df
                  .join(books_df, orders_df.book.book_id == books_df.book_id, "inner")
                  .writeStream
                     .outputMode("append")
                     .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_sales")
                     .trigger(availableNow=True)
                     .table("books_sales")
    )

    query.awaitTermination()
    
process_books_sales()

# COMMAND ----------

# MAGIC %md
# MAGIC We can now review the data in our new table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_sales

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see how many records were written into this new table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM books_sales

# COMMAND ----------

# MAGIC %md
# MAGIC Remember, the stream portion of the join join drives this join process. __So, only new data appearing on the streaming side of the query will trigger the processing.__ And we are guaranteed to get the latest version of the static table during each microbatch transaction. Let us demostrate this by landing a new file in the dataset source directory and propagate the data only to the static table.

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_books_silver()
bookstore.process_current_books()

process_books_sales()

# COMMAND ----------

If we check the number of records in books_sales again, it will remain the same. This confirms the streaming static join did not trigger by only appending data to the static table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM books_sales

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's propagate new data to the orders streaming table and re-execute the streaming-static join

# COMMAND ----------

bookstore.process_orders_silver()

process_books_sales()

# COMMAND ----------

# MAGIC %md
# MAGIC Check the number of records once again below. It has now increased to 4076.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM books_sales

# COMMAND ----------


