from pyspark import pipelines as dp # Import the pipelines module from PySpark
from pyspark.sql import functions as F # As well as the functions module

# Naturally, we need not only define tables, but also functions for custom 
# processing.

# Create a function called process_orders to process the order topics from the 
# multiplex bronze table just created in the first step


def process_orders():
    json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

    return (spark.readStream.table("bronze")
            .filter("topic = 'orders'")
            .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
            .select("v.*"))

# Define the streaming table, orders_silver, by simply calling the process 
# orders function and returning the resulting streaming dataframe.

@dp.table(
    name = "timpomaville.bookstore_etl_pro.orders_silver"
)
# valid_quantity ensures the quantity is greater then zero.
# We are just using the expect decorator here, so we may only expect to 
# see metrics due to constraint violations. Violating records will still 
# be written to the orders table. Running like this, we will see under the 
# tables tab, that we have 24 unmet expectations. Switch to expect_or_fail 
# decoratorator to fail the stream if any expectations are not met. Re-run 
# with full refresh (this will not affect the bronze table) to see the 
# difference, since we set the table property pipelines.reset_allowed to
# false. However, this is too strict for our use case, so we will keep 
# expect decorator but change it to expect_or_drop decorator.

@dp.expect_or_drop("valid_quantity", "quantity > 0")
def orders_silver():
    orders_df = process_orders()
    return orders_df

# Remember, at the silver layer, this is where we apply data quality control.
# This can be achieved here by using data expectations.

# If we want to see these records later for review, we can isolate them in a quarantine table. We # apply the expected or drop function on the negation of our rule. In other words, less than or 
# equal to zero. This will drop the valid records, and write the invalid records to the 
# quarantine table.

@dp.table(
    name = "timpomaville.bookstore_etl_pro.orders_quarantine"
)
@dp.expect_or_drop("valid_quantity", "quantity <= 0")
def orders_quarantine():
    orders_df = process_orders()
    return orders_df

# We can also apply data quality on temporary views. Now, we are creating a temporary view for 
# processing the books topics from the multiplex bronze table. We can add as many expectations 
# as needed: here we add three.

rules = {
    "recent_updates": "updated >= '2020-01-01'",
    "valid_price": "price BETWEEN 0 AND 100",
    "valid_id": "book_id IS NOT NULL",
}

# Next, a common pattern to be used for a case like this is to create a boolean flag. The
# boolean expression is to be the negation of the validation rules comined together.

# With this pattern, you can also partition your table on this column to physically 
# separate invalid data in the underlying table directory.

quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))

@dp.temporary_view
# For reference, naming is not supported for temporary views, apart from 
# whatever the function name (default) is.

# Instead of setting these expectations as we have here, we can group them in a dictionary.
# @dp.expect("recent_updates", "updated >= '2020-01-01'")
# @dp.expect("valid_price", "price BETWEEN 0 AND 100")
# @dp.expect("valid_id", "book_id IS NOT NULL")
@dp.expect_all(rules) # We could of course use expect_all_or_drop here, but we will keep it simple for now.

def books_raw():
    schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"
 
    return (spark.readStream
                    .table("bronze")
                    .filter("topic = 'books'")
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                    .select("v.*")
                    .withColumn("is_quarantined", F.expr(quarantine_rules))
    )

# Data preview is not available for temporary views, but going forward we will see how to 
# persist the results. However, from the table metrics, we can at least confirm that the 
# expect all decorator has correctly defined the three expectations. And, under the columns tab
# we can also confirm the addition of the quarantined column.

# The interface is great, however, the event log can be leveraged to persist the metrics if it
# is required to access the information programatically! Access the SQL Editor to query the event log table.


