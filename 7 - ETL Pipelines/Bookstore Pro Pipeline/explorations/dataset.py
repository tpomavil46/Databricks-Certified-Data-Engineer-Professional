# Databricks notebook source
# MAGIC %run ../../../Includes/Copy-Datasets2

# COMMAND ----------

print(bookstore.dataset_path) # or bookstore.get_dataset_path())

# COMMAND ----------

bookstore.load_pipeline_data()

# COMMAND ----------


