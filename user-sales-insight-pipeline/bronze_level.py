# Databricks notebook source
#Fetch external container location
users_sales_read_location = spark.sql("""
                               DESCRIBE EXTERNAL LOCATION `usersales_external_location`
                               """).select("url").collect()[0][0]
users_sales_write_location = spark.sql("""
                               DESCRIBE EXTERNAL LOCATION `users-sales-bronze`
                               """).select("url").collect()[0][0]
print(users_sales_read_location)
print(users_sales_write_location)


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

# Define schemas
users_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("city", StringType(), True),
    StructField("asset", IntegerType(), True),
    StructField("marital_status", StringType(), True)
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True)
])

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True)
])


# COMMAND ----------

#helper methods
def read_csv(file_path, schema):
    return spark.read.format("csv").option("header", "true").schema(schema).load(file_path)

def write_delta(df, path):
    df.write.format("delta").mode("overwrite").save(path)

def print_schema(delta_path, schema_name):
    print(f"{schema_name} schema:")
    spark.read.format("delta").load(f"{delta_path}").printSchema()


# COMMAND ----------

# read file paths
users_path = f"{users_sales_read_location}users/users_000.csv"
orders_path = f"{users_sales_read_location}orders/orders.csv"
products_path = f"{users_sales_read_location}products/product.csv"

#write file paths
users_delta_path = f"{users_sales_write_location}users/"
orders_delta_path = f"{users_sales_write_location}orders/"
products_delta_path = f"{users_sales_write_location}products/"

# COMMAND ----------

users_df = read_csv(users_path, users_schema)
orders_df = read_csv(orders_path, orders_schema)
products_df = read_csv(products_path, products_schema)

# COMMAND ----------

#writing as delta tables
write_delta(users_df, users_delta_path)
write_delta(orders_df, orders_delta_path)
write_delta(products_df, products_delta_path)

# COMMAND ----------

#verification
print_schema(users_delta_path, "users")
print_schema(products_delta_path, "products")
print_schema(orders_delta_path, "orders")

# COMMAND ----------

print("Bronze layer processing complete.")
