# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------


from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Delta Live Table Creation") \
    .getOrCreate()

# Define source catalog and database
source_catalog = "resellerazsqldb_kl"
source_database = "saleslt"

# Define target Delta catalog and database
target_catalog = "reseller_kl"
target_database = "sales_kl"

# List of source tables
query = f"SHOW TABLES IN {source_catalog}.{source_database}"
table_names = [row.tableName for row in spark.sql(query).collect()]

# Ingest data from foreign catalog
def foreign_table_data():
    query = f"SHOW TABLES IN {source_catalog}.{source_database}"
    tables = [row.tableName for row in spark.sql(query).collect()]
    return {table: spark.read.table(f"{source_catalog}.{source_database}.{table}") for table in tables}

# Rename tables and store in Delta format in the target catalog
def delta_KL(
    table_properties={"quality": "silver"}
):
    table_data = foreign_table_data()
    for table_name, table_df in table_data.items():
        delta_path = f"{target_catalog}.{target_database}/{table_name}_stg"
        table_df.write.format("delta").mode("overwrite").save(delta_path)
    
    return table_data

# COMMAND ----------

@dlt.table
def sales_stg(table_names):
    delta_path = f"{target_catalog}.{target_database}"
    
    # Load and union data from all staging tables
    sales_fact = None
    for table_name in table_names:
        table_df = spark.read.format("delta").load(f"{delta_path}/{table_name}_stg")
        if sales_fact is None:
            sales_fact = table_df
        else:
            sales_fact = sales_fact.union(table_df)
    
    sales_fact = sales_fact.select(
        F.col("SalesOrderID"),
        F.col("ProductID"),
        F.col("ProductCategoryID"),
        F.col("AddressID"),
        F.col("OrderDate"),
        F.col("TotalDue"),
        F.col("SubTotal"),
        F.col("OrderQty")
    )

    # Persist sales fact table
    sales_fact.write.format("delta").mode("overwrite").save(f"{delta_path}/Sales")
    
    return sales_fact

query = f"SHOW TABLES IN {source_catalog}.{source_database}"
table_names = [row.tableName for row in spark.sql(query).collect()]

sales_table = sales_stg(table_names)

# COMMAND ----------

@dlt.table
def product_category_stg():
    delta_table_path = f"{target_catalog}.{target_database}.ProductCategory_stg"

    
    # Load the staging table
    product_category_df = spark.read.format("delta").load(delta_table_path)
    
    categories_dim = product_category_df.select(
        F.col("ProductCategoryID"),
        F.col("ParentProductCategoryName")
    ).distinct()

    # Persist categories dimension table
    categories_dim.write.format("delta").mode("overwrite").save(f"{delta_path}/Categories")

    return categories_dim

product_category_table = product_category_stg()


# COMMAND ----------

@dlt.table
def address_stg():
    delta_table_path = f"{target_catalog}.{target_database}.ProductCategory_stg"
    
    # Load the staging table
    address_df = spark.read.format("delta").load(delta_table_path)
    
    region_dim = address_df.select(
        F.col("AddressID"),
        F.col("CustomerID"),
        F.col("CountryRegion"),
        F.col("StateProvince")
    ).distinct()

    # Persist region dimension table
    region_dim.write.format("delta").mode("overwrite").save(f"{delta_path}/Region")

    return region_dim

address_table = address_stg()


# COMMAND ----------

@dlt.table
def product_stg():
    delta_table_path = f"{target_catalog}.{target_database}.ProductCategory_stg"
    
    # Load the staging table
    product_df = spark.read.format("delta").load(delta_table_path)
    
    product_revenue_dim = product_df.select(
        F.col("ProductID"),
        F.col("UnitPrice")
    ).distinct()
    
    # Persist product revenue dimension table
    product_revenue_dim.write.format("delta").mode("overwrite").save(f"{delta_path}/ProductRevenue")

    return product_revenue_dim

product_table = product_stg()

