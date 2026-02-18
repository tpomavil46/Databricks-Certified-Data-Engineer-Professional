from pyspark import pipelines as dp # Import the pipelines module from PySpark
from pyspark.sql import functions as F # As well as the functions module

# In order to work with CDC, first it is necessary to create a source and target.

@dp.view()
def customers_bronze_cdc():
    schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

    country_lookup_df = spark.read.table("country_lookup")

    return (spark.readStream
                    .table("timpomaville.bookstore_etl_pro.bronze")
                    .filter("topic = 'customers'")
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                    .select("v.*")
                    .filter(F.col("row_status").isin(["insert", "update"]))
                    .join(F.broadcast(country_lookup_df), F.col("country_code") == F.col("code") , "inner")
            )

dp.create_streaming_table(
    name = "timpomaville.bookstore_etl_pro.customers_silver"
)

# Now we can use the auto CDC feature to create a silver table from the bronze table.
dp.create_auto_cdc_flow(
    target = "timpomaville.bookstore_etl_pro.customers_silver",
    source = "customers_bronze_cdc",
    keys = ["customer_id"], # primary key
    sequence_by = F.col("row_time"),
    except_column_list = ["row_status", "row_time"],
    # apply_as_deletes = F.expr("row_status = 'delete'")
)
# At the gold layer, we create our countries statistic view as a materialized view. This joins 
# between customers and orders, data and business level aggregations. Precomputing the results 
# and materializing them will help improve query performance and accelerate BI dashboard reporting.

# A non-streaming dataframe is expected here for materialized views!
@dp.materialized_view(
    name = "countries_stats"
)
def countries_stats():
    orders_df = spark.read.table("timpomaville.bookstore_etl_pro.orders_silver")
    customers_df = spark.read.table("timpomaville.bookstore_etl_pro.customers_silver")

    return (orders_df.join(customers_df, ["customer_id"], "inner")
                        .withColumn("order_date", F.date_trunc("DAY", F.col("order_timestamp")))
                        .groupBy("country", "order_date")
                        .agg(
                            F.count("order_id").alias("orders_count"),
                            F.sum("quantity").alias("books_count")
                        ))
