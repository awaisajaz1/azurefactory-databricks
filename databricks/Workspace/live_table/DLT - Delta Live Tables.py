# Databricks notebook source
__order_Status = spark.conf.get("custom.order_type", "NA")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a new schema 
# MAGIC create schema if not exists external_catalog.etl;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparing source data for DLT 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clone some sample tables from existing catalog
# MAGIC create table if not exists external_catalog.bronze.orders_raw DEEP CLONE samples.tpch.orders;
# MAGIC create table if not exists external_catalog.bronze.customer_raw DEEP CLONE samples.tpch.customer;

# COMMAND ----------

# DLT has three types of datasets
# 1) Streaming tables (permanent/temporary) , used for append data source, incremental data
# 2) Materialize Views, used for transformation, aggregations and computation
# 3) Views, used for transient transformations, not strong the data in target objects

import dlt 

# COMMAND ----------

# create Streaming autoloaders
@dlt.table(
    table_properties={"quality": "bronze"},
    comment="This table contains all the orders from file by using auto loader",
    name="orders_autoloader_bronze"
)
def func():
    df = (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.schemaHints", "o_orderkey int, o_custkey int, o_orderstatus string, o_totalprice double o_orderdate:string, o_orderpriority string, o_clerk string, o_shippriority string, o_comment string"),
        .option("cloudFiles.schemaLocation", "/Volumes/external_catalog/etl/landing_vloume/autoloader/schemas/1")
        .option("CloudFiles.format", "csv")
        .option("cloudFiles.SchemaEvolutionMode", "none")
        .load("/Volumes/external_catalog/etl/landing_vloume/files"
    )
    
    return df.withColumnRenamed('o_custkey', 'customer_id_o')

# COMMAND ----------

# create Streaming table for orders
@dlt.table(
    table_properties={"quality": "bronze"},
    comment="This table contains all the orders"
)
def orders_bronze():
    df = spark.readStream.table("external_catalog.bronze.orders_raw").withColumnRenamed('o_custkey', 'customer_id_o')
    return df

# COMMAND ----------

dlt.create_stream_table("orders_union_bronze")

# Append Flow
@dlt.append(
    target="orders_union_bronze",
)
def orders_delta_append():
    df = spark.readStream.table("Live.orders_bronze")
    return df


# Append Flow
@dlt.append(
    target="orders_union_bronze",
)
def orders_autoloader_append():
    df = spark.readStream.table("Live.orders_autoloader_bronze")
    return df

# COMMAND ----------

# Materialize Views table for customer
@dlt.table(
    table_properties={"quality": "bronze"},
    comment="This table contains all the customers",
    name="customer_bronze"
)
def cust_bronze():
    df = spark.read.table("external_catalog.bronze.customer_raw").withColumnRenamed('c_custkey', 'customer_id_c')
    return df

# COMMAND ----------

# %sql
# -- testing volume only
# select * from csv.`/Volumes/external_catalog/etl/landing_vloume/files/*.csv`

# COMMAND ----------

# Create a view to join the orders with customer
@dlt.view(
    comment="this will be a inmemory view",
)
def cust_orders_joined_vw():
    # df_orders = spark.read.table("LIVE.orders_bronze") now we are using the union table so we are commenting it
    df_orders = spark.read.table("LIVE.orders_union_bronze")
    df_customers = spark.read.table("LIVE.customer_bronze")

    df_joined_view = df_orders.join(df_customers, how="left" ,on=df_orders.customer_id_o == df_customers.customer_id_c)
    return df_joined_view

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer

# COMMAND ----------

# Materialize Views table for customer
from pyspark.sql.functions import current_timestamp

@dlt.table(
    table_properties={"quality": "silver"},
    comment="This table contains all the customers and orders with some new columns ",
    name="orders_staging"
)
def cust_bronze():
    df = spark.read.table("LIVE.cust_orders_joined_vw").withColumn('insert_timestamp', current_timestamp())
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer

# COMMAND ----------

# Aggregation for business stakholders
from pyspark.sql.functions import count, current_timestamp
@dlt.table(
    table_properties={"quality": "gold"},
    comment="This table contains summarized data for business stakeholders - orders"
)
def orders_gold_aggregations():
    df = spark.read.table("LIVE.orders_staging")
    aggregated_df = df.groupBy("c_mktsegmen").agg(count("o_orderkeys").alias("total_orders")).withColumn('insert_timestamp', current_timestamp())
    return aggregated_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic tables using configuration
# MAGIC

# COMMAND ----------

for one_status in __order_Status.split(","):
  @dlt.table(
    table_properties={"quality": "gold"},
    comment="This table contains summarized data for business stakeholders - orders",
    name=f"orders_gold_aggregations_{one_status}"
  )
  def func():
      df = spark.read.table("LIVE.orders_staging")
      aggregated_df = df.where(f"o_orderstatus = '{one_status}'").groupBy("c_mktsegmen").agg(count("o_orderkeys").alias("total_orders")).withColumn('insert_timestamp', current_timestamp())
      return aggregated_df