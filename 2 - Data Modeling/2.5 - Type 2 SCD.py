# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/books.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we will create the books Silver table. For this, we will use a Type 2 SCD table to record the books data. This idea here, is to keep a trace of all the price modifications of a book so we can verify the orders total amount at any given time.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NOTE: This cell contains the SQL query for your reference, and won't work if run directly.
# MAGIC -- The query is used below in the type2_upsert() function as part of the foreachBatch call.
# MAGIC
# MAGIC -- In SCD Type 2, old records need to be marked as no longer valid. For this, we use the book_id as the merge key.
# MAGIC -- So, when there is an update matched with a currently active record in the table, we update the record's current status
# MAGIC -- to false, and we set its end_date to the current timestamp.
# MAGIC -- At the same time, the received updates need to be inserted as separate records. This is why we reprocess them with a 
# MAGIC -- null merge key. This allows us to insert these updates using the NOT MATCHED clause.
# MAGIC -- Also notice, that we are inserting the new records with a current status equal to true, and without end_date.
# MAGIC
# MAGIC MERGE INTO books_silver
# MAGIC USING (
# MAGIC     -- below we are merging our microbatch updates into the books Silver table based on a custom merge key.
# MAGIC     SELECT updates.book_id as merge_key, updates.*
# MAGIC     FROM updates
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT NULL as merge_key, updates.*
# MAGIC     FROM updates
# MAGIC     JOIN books_silver ON updates.book_id = books_silver.book_id
# MAGIC     WHERE books_silver.current = true AND updates.price <> books_silver.price
# MAGIC   ) staged_updates
# MAGIC ON books_silver.book_id = merge_key 
# MAGIC WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
# MAGIC   UPDATE SET current = false, end_date = staged_updates.updated
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (book_id, title, author, price, current, effective_date, end_date)
# MAGIC   VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)

# COMMAND ----------

# MAGIC %md
# MAGIC As before, we are using SQL to write our Delta table.

# COMMAND ----------

def type2_upsert(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("updates")
    
    sql_query = """
        MERGE INTO books_silver
        USING (
            SELECT updates.book_id as merge_key, updates.*
            FROM updates

            UNION ALL

            SELECT NULL as merge_key, updates.*
            FROM updates
            JOIN books_silver ON updates.book_id = books_silver.book_id
            WHERE books_silver.current = true AND updates.price <> books_silver.price
          ) staged_updates
        ON books_silver.book_id = merge_key 
        WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
          UPDATE SET current = false, end_date = staged_updates.updated
        WHEN NOT MATCHED THEN
          INSERT (book_id, title, author, price, current, effective_date, end_date)
          VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)
    """
    
    microBatchDF.sparkSession.sql(sql_query)

# COMMAND ----------

# MAGIC %md
# MAGIC Ensure the table exists before continuing...

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS books_silver
# MAGIC (book_id STRING, title STRING, author STRING, price DOUBLE, current BOOLEAN, effective_date TIMESTAMP, end_date TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can execute a streaming query to process the books data from the bronze table.

# COMMAND ----------

def process_books():
    schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"
 
    query = (spark.readStream
                    .table("bronze")
                    .filter("topic = 'books'")
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                    .select("v.*")
                 .writeStream
                    .foreachBatch(type2_upsert)
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_silver")
                    .trigger(availableNow=True)
                    .start()
            )
    
    query.awaitTermination()
    
process_books()

# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's land some new data and process them.

# COMMAND ----------

bookstore.load_books_updates()
bookstore.process_bronze()
process_books()

# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Back up to cell 13 and process one more file...

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/current_books.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Now, another table called current books to display only the latest books information.

# COMMAND ----------

# MAGIC %md
# MAGIC We use CREATE OR REPLACE syntax, and each time we run the query, the contents of the table will be overwritten.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE current_books
# MAGIC AS SELECT book_id, title, author, price
# MAGIC    FROM books_silver
# MAGIC    WHERE current IS TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_books
# MAGIC ORDER BY book_id

# COMMAND ----------


