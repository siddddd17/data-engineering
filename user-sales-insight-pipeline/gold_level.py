# Databricks notebook source
catalog_name = "unity_catalog_databricks_workspace"
schema_name = "goldlayer_schema"

spark.conf.set("catalog.name",catalog_name)
spark.conf.set("schema.name",schema_name)


# COMMAND ----------

# MAGIC  %sql
# MAGIC  CREATE SCHEMA IF NOT EXISTS `${schema.name}`;
# MAGIC  SELECT current_catalog();
# MAGIC  SELECT current_schema()
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use goldlayer_schema

# COMMAND ----------

from pyspark.sql.functions import * 
users_df = spark.read.table("silverlayer_schema.users")
products_df = spark.read.table("silverlayer_schema.products")
orders_df = spark.read.table("silverlayer_schema.orders")

# COMMAND ----------

users_df.limit(5).display()
orders_df.limit(10).display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# COMMAND ----------

#helper methods
def write_to_table(df, table):
    df.write.format("delta").mode("overwrite").saveAsTable(f"goldlayer_Schema.{table}")
def print_table(table):
    print(f"\nSample rows from {table} table:")
    spark.table(f"goldlayer_Schema.{table}").show(10, truncate=False)

# COMMAND ----------

# Calculate top 10 customers by total amount purchased
top_customers_df = orders_df.groupBy("user_id") \
    .agg(_sum("total_order_amount").alias("total_amount")) \
    .orderBy(col("total_amount").desc()) \
    .limit(10)
top_customers_df.display()

# COMMAND ----------

#create product sales
product_sales_df = orders_df.withColumn("product_id", explode(col("product_id"))) \
    .groupBy("product_id") \
    .agg(round(_sum("total_order_amount"),3).alias("total_sales")) \
    .join(products_df, "product_id") \
    .select("product_id", "product_name", "total_sales") \
    .orderBy(col("total_sales").desc())
product_sales_df.display()

# COMMAND ----------

write_to_table(product_sales_df, "product_sales")
write_to_table(top_customers_df, "top_10_customers")

# COMMAND ----------

#Verification
print_table("product_sales")
print_table("top_10_customers")
