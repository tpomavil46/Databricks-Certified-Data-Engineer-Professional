# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/gold.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Here is a quick overview of how stored views and materialized views can be created in Databricks. For this we can create our two Gold layer entities.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Our view will simply contain some aggregations for daily sales per country. Here we calculate orders count and books count per country and order day. Run the cell to create the view.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VIEW IF NOT EXISTS countries_stats_vw AS (
# MAGIC   SELECT country, date_trunc("DD", order_timestamp) order_date, count(order_id) orders_count, sum(quantity) books_count
# MAGIC   FROM customers_orders
# MAGIC   GROUP BY country, date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Remember that a view is nothing more than an SQL query against tables. So, if you have a complex query with joins and subqueries, each time you query the view, Delta has to scan and join files from multiple tables, which would be very costly. Rerun the query and note how long it takes. Even if you have a query with complex logic, re-executing the view will be super fast on the currently active cluster. In fact, to save costs, Databricks uses a feature called Delta Caching, so that subsequent execution of queries will use cached results. However, this is not guaranteed to be persisted and is only cached for the currently active cluster.
# MAGIC
# MAGIC In traditional databases, usually you can control cost associated with materializing results using materialized views. In Databricks, the concept of a materialized view most closely maps to that of a gold table. Gold tables help cut down the potential cost associated with complex ad-hoc queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries_stats_vw
# MAGIC WHERE country = "France"

# COMMAND ----------

# MAGIC %md
# MAGIC This table stores summary statistics of sales per author. It calculates the orders count and the average quantity per author and per order_timestamp for each non-overlapping five minute interval.
# MAGIC
# MAGIC Furthermore, similar to streaming deduplication, we automatically handle late, out-of-order data, and limit the state using watermarks. Here we have defined a watermark of ten minutes during which incremental state information is maintained for late arriving data.

# COMMAND ----------

from pyspark.sql import functions as F

query = (spark.readStream
                 .table("books_sales")
                 .withWatermark("order_timestamp", "10 minutes")
                 .groupBy(
                     F.window("order_timestamp", "5 minutes").alias("time"),
                     "author")
                 .agg(
                     F.count("order_id").alias("orders_count"),
                     F.avg("quantity").alias ("avg_quantity"))
              .writeStream
                 .option("checkpointLocation", f"dbfs:/mnt/demo_pro/checkpoints/authors_stats")
                 .trigger(availableNow=True)
                 .table("authors_stats")
            )

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Now, reviewing the data written to the new gold table.
# MAGIC
# MAGIC We can now see the author statistics. For a given author, we can view their statistics over a given five minute time window.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM authors_stats

# COMMAND ----------


