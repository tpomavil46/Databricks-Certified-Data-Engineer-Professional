# Databricks notebook source
# MAGIC %md
# MAGIC Here we have a demostration of partitioning on Delta Lake tables. Recal in section 2, that we partitioned the bronze table by two fields: topic and year_month. Take a look at how the data is stored in the table directory. Copy the dataset files...

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC First, we need to know the location of the table. We find below:
# MAGIC Location	dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED bronze

# COMMAND ----------

# MAGIC %md
# MAGIC The top level partition is the topic column. As you can see, we have three partition directories that collectively comprise our bronze table, in addition to the _delta_log directory.
# MAGIC
# MAGIC You can apply access control at the directory level if necessary. For example, on the customers topic directory as it has PII or Personally Identifiable Information data. Let's look into this partition directory.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Our second level partition was on the year_month column. As you can see, there are currently 10 directories present at this level. Remember at these partition boundaries that represent measures of time, you can easily delete or archive old data from the table. Let's go deeper into one of those partition directories.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/topic=customers")
display(files)

# COMMAND ----------

Here, we have nothing but the data files of the partion chosen. In this case they are the data files of the customer topic of this specific month.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/topic=customers/year_month=2021-12/")
display(files)

# COMMAND ----------


