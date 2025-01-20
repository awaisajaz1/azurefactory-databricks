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

sales_src_df.write.mode("overwrite").format("parquet").save("abfss://silverlayer@qoredeltalake.dfs.core.windows.net/branch_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silverlayer@qoredeltalake.dfs.core.windows.net/branch_sales`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now the data is in Silver for creating the dimension

# COMMAND ----------

df_src = spark.sql('''
          select distinct Branch_ID, BranchName 
          from parquet.`abfss://silverlayer@qoredeltalake.dfs.core.windows.net/branch_sales`
          ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now we shall create the table from here by checking if exist, 

# COMMAND ----------

if not spark.catalog.tableExists('external_catalog.gold.branch_dimension'):
    df_sink = spark.sql('''
                        select 1 Branch_SK, Branch_ID, BranchName 
                        from parquet.`abfss://silverlayer@qoredeltalake.dfs.core.windows.net/branch_sales`
                        where 1 = 2
                        ''')
else:
    df_sink = spark.sql('''
                        select  Branch_SK, Branch_ID, BranchName 
                        from external_catalog.gold.branch_dimension
                        ''')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## We shall keep only New Records and Updated Records,

# COMMAND ----------

df_join_source_target = df_src.join(df_sink, df_src.Branch_ID == df_sink.Branch_ID, 'left')\
   .select(\
        df_sink.Branch_SK,
        df_sink.Branch_ID.alias("Branch_ID_Sink"),
        df_src.Branch_ID,
        df_src.BranchName,
        df_sink.BranchName.alias("BranchName_Sink"),
    ).withColumn(
        "Dataset_Filtering",
        expr("""
            CASE 
                WHEN Branch_ID_Sink IS NULL THEN 'New Record'
                WHEN Branch_ID_Sink IS NOT NULL AND BranchName <> BranchName_Sink THEN 'Updated Record'
                ELSE 'Rejected Records'
            END
        """)
    )

df_join_source_target = df_join_source_target.filter(
    (col("Dataset_Filtering") == "New Record") | (col("Dataset_Filtering") == "Updated Record")
)\
.drop(
    "Branch_ID_Sink",
    "BranchName_Sink"
)
df_join_source_target.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Surrogate Key

# COMMAND ----------

df_surrogatekey = 1
if  spark.catalog.tableExists('external_catalog.gold.branch_dimension'):
    df_sk = spark.sql('''
                        select max(Branch_SK) max_sk
                        from external_catalog.gold.branch_dimension
                      ''')
    df_surrogatekey = df_sk.collect()[0][0] + 1
else:
    df_surrogatekey 
  
print(df_surrogatekey)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from delta.`abfss://goldlayer@qoredeltalake.dfs.core.windows.net/dimensions/branch_dimension`

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 1

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table external_catalog.gold.branch_dimension
# MAGIC -- select * from external_catalog.gold.branch_dimension

# COMMAND ----------

## Initial Run
if not spark.catalog.tableExists('external_catalog.gold.branch_dimension'):
    gold_dim = df_join_source_target.withColumn(
        'Branch_SK',
        df_surrogatekey+ monotonically_increasing_id()
    ).drop("Dataset_Filtering")
    
    gold_dim.write\
    .mode("overwrite")\
    .format("delta")\
    .option("path", "abfss://goldlayer@qoredeltalake.dfs.core.windows.net/dimensions/branch_dimension")\
    .saveAsTable("external_catalog.gold.branch_dimension")

## Incremental Run by creating the source and target and then comapre them to see if there is any change
elif spark.catalog.tableExists('external_catalog.gold.product_dimension'):
    # gold_delta_tbl = DeltaTable.forName(spark, "external_catalog.gold.gold_product_dimension")
    # Load Delta table from the specified path
    gold_delta_tbl = DeltaTable.forPath(
        spark, 
        "abfss://goldlayer@qoredeltalake.dfs.core.windows.net/dimensions/branch_dimension"
    )


    # Generate surrogate key for new records
    df_with_sk = df_join_source_target.filter(
            (col("Dataset_Filtering") == "New Record")
        ).withColumn(
        'Branch_SK',
        df_surrogatekey + monotonically_increasing_id()
    ).drop("Dataset_Filtering")
        
    # these are old records that donot require sk generation
    df_without_sk = df_join_source_target.filter(
            (col("Dataset_Filtering") == "Updated Record")
        ).withColumn(
        'Branch_SK', lit(None).cast(IntegerType())
    ).drop("Dataset_Filtering")

    df_union = df_with_sk.union(df_without_sk)
    # df_union.display()


    gold_delta_tbl.alias("tgt")\
    .merge(
        df_union
        .alias("src"),
        "tgt.Branch_ID = src.Branch_ID"
    )\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

    # # ## Scenrio 1, When key matching but column has update from source to target to close old records
    # gold_delta_tbl.alias("tgt")\
    # .merge(
    #     df_join_source_target.filter(
    #         (col("Dataset_Filtering") == "Updated Record") | (col("Dataset_Filtering") == "New Record")
    #     )
    #     .drop("Dataset_Filtering")
    #     .alias("src"),
    #     "tgt.Branch_ID = src.Branch_ID"
    # )\
    # .whenMatchedUpdateAll()\
    # .whenNotMatchedInsert(
    #     values={
    #         "Branch_ID": "src.Branch_ID",
    #         "Branch_SK": (df_surrogatekey + monotonically_increasing_id()),
    #         "BranchName": "src.BranchName"
    #     }
    # )\
    # .execute()




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from external_catalog.gold.branch_dimension;
# MAGIC -- delete from external_catalog.gold.branch_dimension where Branch_SK is null;
# MAGIC
# MAGIC -- update external_catalog.gold.product_dimension
# MAGIC -- set End_Time = null
# MAGIC -- where Model_SK = 250;
# MAGIC
# MAGIC
# MAGIC