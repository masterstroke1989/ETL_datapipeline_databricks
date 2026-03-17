# Databricks notebook source
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL_Pipleline_Real").getOrCreate()

# Store raw data exactly as received.

customers_df = spark.read.csv(
    "/Workspace/Users/tushar.kulkarni89@gmail.com/ETL_datapipeline_databricks/customers.csv",
    header=True,
    inferSchema=True
)
# customers_df.show()

products_df = spark.read.csv(
    "/Workspace/Users/tushar.kulkarni89@gmail.com/ETL_datapipeline_databricks/products.csv",
    header=True,
    inferSchema=True
)
# products_df.show()

orders_df = spark.read.json(
    "/Workspace/Users/tushar.kulkarni89@gmail.com/ETL_datapipeline_databricks/orders.json"
)
# orders_df.show()

# Save as Delta tables:
customers_df.write.format("delta").mode("overwrite").saveAsTable("bronze_customers")
products_df.write.format("delta").mode("overwrite").saveAsTable("bronze_products")
orders_df.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")