# Databricks notebook source
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL_Pipleline_Real").getOrCreate()

# Load Silver table:
sales = spark.table("silver_sales")

# Customer revenue:
customer_sales = sales.groupBy("customer_id","name") \
    .agg(sum("sales_amount").alias("total_spent"))

# Save Gold table:
customer_sales.write.format("delta") \
.mode("overwrite") \
.saveAsTable("gold_customer_sales")


