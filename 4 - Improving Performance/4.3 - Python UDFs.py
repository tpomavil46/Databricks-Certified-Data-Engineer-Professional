# Databricks notebook source
# MAGIC %md
# MAGIC In this notebook, we explore Python User-Defined Functions (UDFs) in spark user defined functions. UDFs provide the means to make custom column transformations. We continue to use our books table.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

df_books = spark.table("books_silver")
display(df_books)

# COMMAND ----------

# MAGIC %md
# MAGIC Here we have a simple, classical, python function to apply a discount percentage on a price.

# COMMAND ----------

def apply_discount(price, percentage):
    return price * (1 - percentage/100)

# COMMAND ----------

apply_discount(100, 20)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, in order to use it on a pyspark dataframe, we need to register it as a UDF.

# COMMAND ----------

apply_discount_udf = udf(apply_discount)

# COMMAND ----------

# MAGIC %md
# MAGIC The first paramter is the price, and the second is a literal value of 50 to apply a 50% discount.

# COMMAND ----------

from pyspark.sql.functions import col, lit

df_discounts = df_books.select("price", apply_discount_udf(col("price"), lit(50)))
display(df_discounts)

# COMMAND ----------

# MAGIC %md
# MAGIC This is cool, but it cannot be used in SQL. Instead, we need to use another function to declare the UDF. This makes the function available for use in bopth Python and SQL.

# COMMAND ----------

apply_discount_py_udf = spark.udf.register("apply_discount_sql_udf", apply_discount)

# COMMAND ----------

# MAGIC %md
# MAGIC In Python

# COMMAND ----------

df_discounts = df_books.select("price", apply_discount_py_udf(col("price"), lit(50)))
display(df_discounts)

# COMMAND ----------

# MAGIC %md
# MAGIC And in SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT price, apply_discount_sql_udf(price, 50) AS price_after_discount
# MAGIC FROM books_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively, you can define and register a UDF using Python decorator syntax. The UDF decorator parameter is the column data type the function returns. It means our UDFs input and output is of type double.

# COMMAND ----------

@udf("double")
def apply_discount_decorator_udf(price, percentage):
    return price * (1 - percentage/100)

# COMMAND ----------

# MAGIC %md
# MAGIC Because we used the decorator above, we can no longer call the local python function.

# COMMAND ----------

apply_discount_decorator_udf(100, 20)

# COMMAND ----------

# MAGIC %md
# MAGIC Now using the decorator UDF on the dataframe.

# COMMAND ----------

df_discounts = df_books.select("price", apply_discount_decorator_udf(col("price"), lit(50)))
display(df_discounts)

# COMMAND ----------

# MAGIC %md
# MAGIC The problem, however, with Python decorator UDFs is that they cannot be optimized by Spark. And in addition, there is a cost of transferring data between the spark engine and the Python interpreter.
# MAGIC
# MAGIC One way to improve the efficiency of UDFs in Python is to use Pandas UDFs. These are vectorized UDFs that use Apache Arrow format. Apache Arrow is an in-memory data format that enables fast data transfers between Sparks JVM and Python runtimes, which reduces computational and serialization costs.
# MAGIC
# MAGIC Notice below we are using a Pandas UDF function to define our vectorized UDF, and specify that the input and the output are of type double.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

def vectorized_udf(price: pd.Series, percentage: pd.Series,) -> pd.Series:
    return price * (1 - percentage/100)

vectorized_udf = pandas_udf(vectorized_udf, "double")

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively, it can be declared using decorator syntax.

# COMMAND ----------

@pandas_udf("double")
def vectorized_udf(price: pd.Series, percentage: pd.Series,) -> pd.Series:
    return price * (1 - percentage/100)

# COMMAND ----------

df_domains = df_books.select("price", vectorized_udf(col("price"), lit(50)))
display(df_domains)

# COMMAND ----------

# MAGIC %md
# MAGIC You can also register pandas UDFs to the SQL namespace.

# COMMAND ----------

spark.udf.register("sql_vectorized_udf", vectorized_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT price, sql_vectorized_udf(price, 50) AS price_after_discount
# MAGIC FROM books_silver

# COMMAND ----------


