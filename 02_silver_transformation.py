# Databricks notebook source
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL_Pipleline_Real").getOrCreate()

# Load Bronze tables:
customers = spark.table("bronze_customers")
products = spark.table("bronze_products")
orders = spark.table("bronze_orders")

# Data cleaning:
customers_clean = customers.dropDuplicates(["customer_id"])
products_clean = products.filter(col("price") > 0)
orders_clean = orders.filter(col("quantity") > 0)

# Join datasets:
sales_df = orders_clean.join(customers_clean, "customer_id") \
                       .join(products_clean, "product_id")

# Calculate sales amount:
sales_df = sales_df.withColumn(
    "sales_amount",
    col("price") * col("quantity")
)

# Save Silver table:
sales_df.write.format("delta") \
.mode("overwrite") \
.saveAsTable("silver_sales")

