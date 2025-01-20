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

# MAGIC %md
# MAGIC ## Test the Visualization

# COMMAND ----------

sales_src_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation to create a dimension

# COMMAND ----------

df_intermediate_process = sales_src_df.withColumn('Model_Categroy', split(col('Model_ID'), '-')[0])
df_intermediate_process.display()
df_intermediate_process.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Random Testing

# COMMAND ----------

df_intermediate_process.groupBy(col('Year'),col('Model_Categroy')).agg(sum(col('Revenue')*col('Units_Sold')).alias('Total_Revenue')).sort(col('Year'), col('Model_Categroy'), ascending=[1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Above we were testing some transformation, but now we shall over write in silver layer

# COMMAND ----------

sales_src_df.write.mode("overwrite").format("parquet").save("abfss://silverlayer@qoredeltalake.dfs.core.windows.net/car_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silverlayer@qoredeltalake.dfs.core.windows.net/car_sales`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now the data is in Silver for creating the dimension

# COMMAND ----------

df_src = spark.sql('''
          select distinct Model_ID, Product_Name 
          from parquet.`abfss://silverlayer@qoredeltalake.dfs.core.windows.net/car_sales`
          ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now we shall create the table from here by checking if exist, 

# COMMAND ----------

if not spark.catalog.tableExists('external_catalog.gold.product_dimension'):
    df_sink = spark.sql('''
                        select 1 Model_SK, Model_ID, Product_Name 
                        from parquet.`abfss://silverlayer@qoredeltalake.dfs.core.windows.net/car_sales`
                        where 1 = 2
                        ''')
else:
    df_sink = spark.sql('''
                        select  Model_SK, Model_ID, Product_Name 
                        from external_catalog.gold.product_dimension
                        where End_Time IS NULL
                        ''')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## We shall keep only New Records and Updated Records,

# COMMAND ----------

df_join_source_target = df_src.join(df_sink, df_src.Model_ID == df_sink.Model_ID, 'left')\
   .select(\
        df_sink.Model_SK,
        df_sink.Model_ID.alias("Model_ID_Sink"),
        df_src.Model_ID,
        df_src.Product_Name,
        df_sink.Product_Name.alias("Product_Name_Sink"),
    )\
    .withColumn('Start_Time', current_timestamp())\
    .withColumn('End_Time', lit(None).cast(TimestampType()) )\
    .withColumn(
        "Dataset_Filtering",
        expr("""
            CASE 
                WHEN Model_ID_Sink IS NULL THEN 'New Record'
                WHEN Model_ID_Sink IS NOT NULL AND Product_Name <> Product_Name_Sink THEN 'Updated Record'
                ELSE 'Rejected Records'
            END
        """)
    )

df_join_source_target = df_join_source_target.filter(
    (col("Dataset_Filtering") == "New Record") | (col("Dataset_Filtering") == "Updated Record")
)\
.drop(
    "Model_ID_Sink",
    "Product_Name_Sink"
)
df_join_source_target.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Surrogate Key

# COMMAND ----------

df_surrogatekey = 1
if  spark.catalog.tableExists('external_catalog.gold.product_dimension'):
    df_sk = spark.sql('''
                        select max(Model_SK) max_sk
                        from external_catalog.gold.product_dimension
                      ''')
    df_surrogatekey = df_sk.collect()[0][0] + 1
else:
    df_surrogatekey 
  
print(df_surrogatekey)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://goldlayer@qoredeltalake.dfs.core.windows.net/dimensions/product_dimension`

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table external_catalog.gold.product_dimension
# MAGIC select * from external_catalog.gold.product_dimension

# COMMAND ----------

## Initial Run
if not spark.catalog.tableExists('external_catalog.gold.product_dimension'):
    gold_dim = df_join_source_target.withColumn(
        'Model_SK',
        df_surrogatekey+ monotonically_increasing_id()
    ).drop("Dataset_Filtering")
    
    gold_dim.write\
    .mode("overwrite")\
    .format("delta")\
    .option("path", "abfss://goldlayer@qoredeltalake.dfs.core.windows.net/dimensions/product_dimension")\
    .saveAsTable("external_catalog.gold.product_dimension")

## Incremental Run by creating the source and target and then comapre them to see if there is any change
elif spark.catalog.tableExists('external_catalog.gold.product_dimension'):
    # gold_delta_tbl = DeltaTable.forName(spark, "external_catalog.gold.gold_product_dimension")
    # Load Delta table from the specified path
    gold_delta_tbl = DeltaTable.forPath(
        spark, 
        "abfss://goldlayer@qoredeltalake.dfs.core.windows.net/dimensions/product_dimension"
    )



    ## Scenrio 1, When key matching but column has update from source to target to close old records
    gold_delta_tbl.alias("tgt")\
    .merge(
        df_join_source_target.filter(col("Dataset_Filtering") == "Updated Record")
                  .drop("Dataset_Filtering")
                  .alias("src"),
        "tgt.Model_ID = src.Model_ID"
    )\
    .whenMatchedUpdate(set={
        "End_Time": "current_timestamp()"
    })\
    .execute()
    
    ## Scenrio 2, Inserty Everything with new surrogate key
    # df_surrogatekey+= monotonically_increasing_id()
    gold_delta_tbl.alias("tgt") \
        .merge(
            df_join_source_target.withColumn('Model_SK', df_surrogatekey + monotonically_increasing_id()).alias("src"),
            "tgt.Model_ID = src.Model_ID and tgt.End_Time IS NULL"
        ) \
        .whenNotMatchedInsertAll() \
        .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from external_catalog.gold.product_dimension;
# MAGIC -- delete from external_catalog.gold.product_dimension where product_name in( 'NEW PRODUCT', 'BMW_Updated');
# MAGIC
# MAGIC -- update external_catalog.gold.product_dimension
# MAGIC -- set End_Time = null
# MAGIC -- where Model_SK = 250;
# MAGIC
# MAGIC
# MAGIC