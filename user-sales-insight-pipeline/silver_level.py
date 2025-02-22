# Databricks notebook source
#fetch external location
base_bronze_path = spark.sql("""
                               DESCRIBE EXTERNAL LOCATION `users-sales-bronze`
                               """).select("url").collect()[0][0]
print(base_bronze_path)                              

# COMMAND ----------

users_bronze_path = f"{base_bronze_path}users/"
orders_bronze_path = f"{base_bronze_path}orders/"
products_bronze_path = f"{base_bronze_path}products/"

# COMMAND ----------

catalog_name = "unity_catalog_databricks_workspace"
schema_name = "silverlayer_schema"

spark.conf.set("catalog.name",catalog_name)
spark.conf.set("schema.name",schema_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS `${schema.name}`;
# MAGIC SELECT current_catalog();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use silverlayer_schema

# COMMAND ----------

# Read bronze tables
users_df = spark.read.format("delta").load(users_bronze_path)
orders_df = spark.read.format("delta").load(orders_bronze_path)
products_df = spark.read.format("delta").load(products_bronze_path)

# COMMAND ----------

#preview dataframes for verification
users_df.display()
orders_df.limit(5).display()
products_df.limit(5).display()

# COMMAND ----------

# Perform transformations
# Modify users table: drop rows with null values
users_silver = users_df.dropna()
users_silver.display()

# COMMAND ----------

# 2. Modify orders table
from pyspark.sql.functions import *
orders_silver = (orders_df.dropna()
    .withColumn("order_amount", col("order_amount") * col("quantity"))
    .groupBy("order_id", "user_id", "order_date")
    .agg(
        collect_set("product_id").alias("product_id"),
        sum("order_amount").alias("total_order_amount"),
        sum("quantity").alias("quantity")
    )
)
orders_silver.dropna().display()

# COMMAND ----------

products_silver = products_df

# COMMAND ----------

#helper methods
def write_to_table(df, table):
    df.write.format("delta").mode("overwrite").saveAsTable(f"silverlayer_schema.{table}")
def print_table(table):
    print(f"\nSample rows from {table} table:")
    spark.table(f"silverlayer_schema.{table}").show(10, truncate=False)

# COMMAND ----------

write_to_table(users_silver, "users")
write_to_table(orders_silver, "orders")
write_to_table(products_silver, "products")

# COMMAND ----------

print_table("users")
print_table("orders")
print_table("products")

# COMMAND ----------


print("Silver layer processing complete.")
