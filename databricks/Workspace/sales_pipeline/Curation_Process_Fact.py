# Databricks notebook source
# MAGIC %md
# MAGIC ## Read a Parquet Incremnetal File From Bronze Layer
# MAGIC ###### This is a mock development to demonstrate a concept to the newbies. We are extracting the dimension columns from transaction files; otherwise, lookups typically have their own dedicated objects. There are many ways to do it but just to show the logic we are trying this way!!
# MAGIC
# MAGIC ###### Some steps were written verbosely to show the changes; otherwise, they could have been done in a more intuitive way.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

sales_src_df = spark.read.format("parquet")\
  .option('inferSchema', 'true')\
    .load("abfss://bronzelayer@qoredeltalake.dfs.core.windows.net/car_sales")

# COMMAND ----------

sales_src_df.display()

# COMMAND ----------

sales_src_df.write.mode("overwrite").format("parquet").save("abfss://silverlayer@qoredeltalake.dfs.core.windows.net/sales_transaction")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silverlayer@qoredeltalake.dfs.core.windows.net/sales_transaction`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now the data is in Silver for creating the dimension

# COMMAND ----------

df_src = spark.sql('''
          select * 
          from parquet.`abfss://silverlayer@qoredeltalake.dfs.core.windows.net/sales_transaction`
          ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

## Load Dimension Dataframes

# COMMAND ----------

df_product_dim = spark.sql("select * from external_catalog.gold.product_dimension")
df_brach_dim = spark.sql("select * from external_catalog.gold.branch_dimension")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactional Fact with lowest level of grain

# COMMAND ----------

df_silver = df_src.join(df_product_dim, df_src.Model_ID == df_product_dim.Model_ID, how='left')\
            .join(df_brach_dim, df_src.Branch_ID == df_brach_dim.Branch_ID, how='left') \
            .select(df_src.Branch_ID,
                    df_brach_dim.Branch_SK,
                    df_src.Model_ID,
                    df_product_dim.Model_SK,
                    df_src.Revenue, 
                    df_src.Units_Sold,
                    (col("Revenue") * col("Units_Sold")).alias("Total_Revenue"),
                    df_src.Day,
                    df_src.Month, 
                    df_src.Year,
                    df_src.Date_ID, 
                    df_src.DealerName)
df_silver.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

## Initial Run
if not spark.catalog.tableExists('external_catalog.gold.sales_fact'):
    df_silver.withColumn("Process_Type", lit("Insert")).write\
    .mode("overwrite")\
    .format("delta")\
    .option("path", "abfss://goldlayer@qoredeltalake.dfs.core.windows.net/fact_objects/sales_fact")\
    .saveAsTable("external_catalog.gold.sales_fact")

## Incremental Run by creating the source and target and then comapre them to see if there is any change
elif spark.catalog.tableExists('external_catalog.gold.sales_fact'):
    gold_delta_tbl = DeltaTable.forName(
        spark, 
        "external_catalog.gold.sales_fact"
    )
    # Deduplicate the source DataFrame
    # df_silver_dedup = df_silver.dropDuplicates(["Branch_SK", "Model_SK", "DealerName", "Date_ID"])

    df_silver_dedup.display()

    gold_delta_tbl.alias("tgt").merge(df_silver.alias("src"), "tgt.Branch_SK = src.Branch_SK AND tgt.Model_SK = src.Model_SK AND tgt.DealerName = src.DealerName AND tgt.Date_ID = src.Date_ID")\
        .whenMatchedUpdate(set= {"Process_Type": lit("Update")})\
        .whenNotMatchedInsert(values= {"Process_Type": lit("Insert")})\
        .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from external_catalog.gold.sales_fact;
# MAGIC -- delete from external_catalog.gold.branch_dimension where Branch_SK is null;
# MAGIC
# MAGIC -- update external_catalog.gold.product_dimension
# MAGIC -- set End_Time = null
# MAGIC -- where Model_SK = 250;
# MAGIC
# MAGIC -- drop table external_catalog.gold.sales_fact;
# MAGIC
# MAGIC
# MAGIC -- select Branch_SK, Model_SK, count(1) from external_catalog.gold.sales_fact
# MAGIC -- group by Branch_SK, Model_SK
# MAGIC -- -- having count(1) > 1;
# MAGIC
# MAGIC

# COMMAND ----------

# df_silver.createOrReplaceTempView("silver_view")

# COMMAND ----------

# spark.sql("select Branch_SK, Model_SK, count(1) from silver_view group by Branch_SK, Model_SK").display()