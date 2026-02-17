# Databricks notebook source
# MAGIC %md
# MAGIC In this notebook, we explore the Delta Lake Transaction Log. 

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC To begin, we start by reviewing Delta Lake File Statistics. Those statistics are recorded within the transaction log files. Remember, transaction log files are located in the _delta_log directory within the table location. Let's take a look at the _delta_log of our bronze table.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's query one of these json log files directly using Spark. In the add column, we see the list of added files within this commit. The Delta Lake statistics are available in this add column. The statistics include the number of records in the file. As well as the MIN, MAX, and the count of null values for each column in the bronze table.
# MAGIC
# MAGIC Remember, these statistics are limited to the first 32 columns int he table. Free text, such as user feedback and reviews, should be kept outside the range of the first 32 columns to maintain optimal performance. These stats are captured per data file. Each add entry will contain different stats.
# MAGIC
# MAGIC So, when a query with a selective filter, I mean with a WHERE clause is executed against the table, the query optimizer user these stats to generate the query results.
# MAGIC
# MAGIC More precisely, it leverages them to identify data files that may contain records mathing the conditional filter. For example, if in the query you are filtering on an offset greater than the maximum offset value in the file, Delta Lake will skip the file.

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log/00000000000000000001.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC In addition, file statistics can be leveraged directly. For example, when querying the total number of records in a table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Even if you have a huge table, you will notice that querying its count will be super fast. Delta Lake does not need to calculate the count by scanning all the data files. Instead it uses the information contained in the transaction log to return the query results. Moving on, we review the transaction log checkpoints.
# MAGIC
# MAGIC Let us list once more the files in the delta log directory of our bronze table. In the case below, we have not reached or exceeded ten commits, so we do not yet have a parquet file to accompany the json files for what would otherwise be ten. But, if we had, they are used to accelerate the resolution of the current table state.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC If we had it available, we would query this file as seen below. Rather than showing the operations of the most recent transaction, the checkpoint file condenses all of the adds and remove instructions and valid metadata into a single file. This means that in order to find the data files currently representing the valid table version, there is no need to load many JSON files and comparing those data files in the add and remove columns. Only this single file can be loaded that fully describes the table state.

# COMMAND ----------

display(spark.read.parquet("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log/00000000000000000010.checkpoint.parquet"))

# COMMAND ----------

# MAGIC %md
# MAGIC If you look back at the delta log after this checkpoint has already been established, there nay be new json files that have been added to the delta log. Spark will automatically read these files as the starting point so it resolves the new info from these files first with the instruction from the parquet snapshot.

# COMMAND ----------


