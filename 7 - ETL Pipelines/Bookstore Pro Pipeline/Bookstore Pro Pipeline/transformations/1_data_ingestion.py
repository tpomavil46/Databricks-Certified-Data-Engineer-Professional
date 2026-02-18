from pyspark import pipelines as dp # Import the pipelines module from PySpark
from pyspark.sql import functions as F # As well as the functions module

dataset_path = spark.conf.get("dataset_path") # read the dataset_path parameter that was defined in the pipeline settings

# Proceed to create the first object (the multiplex bronze table). It is created as a streaming table table to support 
# incremental data ingestion from an autoloader stream.

# This function defines an autoloader stream and returns a streaming dataframe. Notice we do not perform a streaming
# write in LDP, rather it is automatically handled by the table decorator.

# By default, the table decorator will create a table with the same name as the function. However, we can override this
# by passing a table name as an argument to the decorator.

# The table will be created in the default catalog and schema that you define in the pipeline settings, but you can
# use the three level namespace to persist your object to a different catalog or schema. For example, you may have a schema for 
# each layer, bronze, silver, and gold.

@dp.table (
    name = "timpomaville.bookstore_etl_pro.bronze", # You can add additional parameters here to control the table creation
    partition_cols=["topic", "year_month"],
    table_properties={"delta.appendOnly": "true", # disbale updates and deletes
                      "pipelines.reset.allowed": "false", # data and checkpoints will not be affected by a full refresh
                      }
)
def process_bronze():
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    bronze_df = (spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .schema(schema)
                    .load(f"{dataset_path}/kafka-raw-etl")
                    .withColumn("timestamp", (F.col("timestamp") / 1000).cast("timestamp"))
                    .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                )
    return bronze_df

# We can also set up temporary views to support queries against the streaming tables. This is useful for testing and 
# debugging. 

@dp.temporary_view
def country_lookup():
    countries_df = spark.read.json(f"{dataset_path}/country_lookup")
    return countries_df

#Before running, do not forget that we have the ability here to perform a dry run.
