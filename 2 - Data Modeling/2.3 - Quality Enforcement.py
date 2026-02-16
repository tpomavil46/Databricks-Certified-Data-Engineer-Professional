# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Next, I'll demonstrate how to add check constraints to delta tables to ensure the quality of the data being ingested into the table.
# MAGIC
# MAGIC Table constraints apply boolean filters to columns, and prevent data violating constraints from being written.
# MAGIC
# MAGIC We can add constraints on existing delta tables using the __ALTER TABLE_ADD CONSTRAINT__ command. Be sure to apply a human readable constraint name, in our case, timestamp_within_range, followed by a boolean condition to be checked.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT timestamp_within_range CHECK (order_timestamp >= '2020-01-01');

# COMMAND ----------

# MAGIC %md
# MAGIC __Notice that the condition of a check looks like a standard WHERE clause you might use.__

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC See what happens when we try to insert new data, where one of the data points violates the constraint.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_silver
# MAGIC VALUES ('1', '2022-02-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('2', '2019-05-01 00:00:00.000', 'C00001', 0, 0, NULL), -- violation!
# MAGIC        ('3', '2023-01-01 00:00:00.000', 'C00001', 0, 0, NULL)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that ACID guarentees on Delta Lake that all transactions are atomic, that is they succeed or fail completely. None of the transactions above have been inserted, even if some do not violate the constraints. Confirming below

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC WHERE order_id IN ('1', '2', '3')

# COMMAND ----------

# MAGIC %md
# MAGIC Adding yet another constraint...since in our dataset, we know that some bad orders are sent, by error, with a quantity of zero items. So, we add a check that quantity must be greater then zero.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT valid_quantity CHECK (quantity > 0);

# COMMAND ----------

# MAGIC %md
# MAGIC The command above fails with this error that some rows in the table violate the new Check Constraint. __In fact, ADD CONSTRAINT command verifies that all existing rows satisfy the constraint before adding it to the table__. Makes sense...and as expected, the new constraint has not been added to the table. So, you __MUST__ first ensure that no data violating the constraint is already in the table prior to defining any one constraint.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC So, take a look at the table's data. We have 24 orders with sero items in them. So, now how do we deal with this? We could manually delete these bad records and add the constraint, or set the constraint before processing the data from the bronze table.
# MAGIC
# MAGIC However, as we saw with the above timestamp constraint, if a batch of data contains records that violate the constraint, the job will fail throw an error. __If our goal is to exclude bad records but keep streaming jobs running, we will need a different solution__.
# MAGIC
# MAGIC We could separate such bad records into a quarantine table for example, or simply filter them out before performing the write into the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC where quantity <= 0

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
        .filter("quantity > 0") # as discussed above, adding filter to the quantity field.
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Last, if we need to remove a constraint from a table, we can use DROP CONSTRAINT command.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver DROP CONSTRAINT timestamp_within_range;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC For now, we drop the table...

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ...and delte the checkpoint.

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo_pro/checkpoints/orders_silver", True)

# COMMAND ----------


