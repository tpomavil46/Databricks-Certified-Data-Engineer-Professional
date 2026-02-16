# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC This example below is for batch ingestion of raw data into a bronze table, using:
# MAGIC 1. use dbutils.fs.ls to list the files in the kafka-raw directory
# MAGIC 2. use spark.read.json to read the files into a dataframe
# MAGIC 3. use spark.readStream to read the files into a dataframe in a batch streaming mode
# MAGIC 4. use spark.table to read the dataframe into a table
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")
display(files)

# COMMAND ----------

df_raw = spark.read.json(f"{dataset_bookstore}/kafka-raw")
display(df_raw)

# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
  
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (spark.readStream
                        .format("cloudFiles") # specifies the stream using cloudFiles format
                        .option("cloudFiles.format", "json") # specify the format
                        .schema(schema) # specify the schema
                        .load(f"{dataset_bookstore}/kafka-raw")
                        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  # convert timestamp to datetime
                        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM")) # add year_month columns
                  .writeStream
                      .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bronze")
                      .option("mergeSchema", True) # leverages schema evolution functionality of auto-loader
                      .partitionBy("topic", "year_month") # We next partion by the topic, then the year_month fields
                      .trigger(availableNow=True) # process all the data in the stream that is available right now in batch mode, then stops on it's own.
                      .table("bronze"))
    
    query.awaitTermination()

# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze") # easily create a dataframe using the spark.table function
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(topic) 
# MAGIC FROM bronze

# COMMAND ----------

bookstore.load_new_data() # process a new file into the stream

# COMMAND ----------

process_bronze() # call our process_bronze function to process the new data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze
