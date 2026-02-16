# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/CDF.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC In this note book, we see how easy it is to propagate changes through a lakehouse with Delta Lake Change Data Feed (CDF).

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC We start by enabling CDF on our exisiting customers_silver table.
# MAGIC
# MAGIC We use the ALTER TABLE command to set the property: __delta.enableChangeDataFeed = true__

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customers_silver 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at the table history, we can see that we have set the table properties for CDF to true starting with version 2. Note the version number as we will use it later for reading the CDF data.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Now we will land new data files and process them to our customers_silver table.

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC Let us now read the changes recorded by the CDF after these new data are merged into the customers table.
# MAGIC
# MAGIC In SQL, we use the table changes function where we pass the table name and the starting version (we can use the starting timestamp as well).
# MAGIC
# MAGIC After running the command below, we can see there are two change types currently recorded, in addition to inserts. Update_preimage and update_postimage. We left out deletes as stated in the last notebook, for the time being.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("customers_silver", 2)

# COMMAND ----------

# MAGIC %md
# MAGIC To continue, we can land another file in the source directory and process it.

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.process_orders_silver()
bookstore.process_customers_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC In python, we can read the recorded data by adding two options to a read stream query which are readChangeData = true, and startingVersion. Here we do a streaming display to see the query results.
# MAGIC
# MAGIC __Dont forget to cancel the stream__

# COMMAND ----------

cdf_df = (spark.readStream
               .format("delta")
               .option("readChangeData", True)
               .option("startingVersion", 2)
               .table("customers_silver"))

display(cdf_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the table directory in the underlying storage. The is now an additional metadata directory nested in the table directory called _change_data. Let's now look into this folder...

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/customers_silver")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC This directory now contains parquet files for the change data feed.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/customers_silver/_change_data")
display(files)

# COMMAND ----------


