#!/usr/bin/env python
# coding: utf-8

# ## Silver Transformations
# 
# New notebook

# In[1]:


from pyspark.sql.functions import *
from pyspark.sql.types import *


# In[2]:


# Check if silver table already exists

if not spark.catalog.tableExists('Lakehouses.sales_silver'):
    print("Table Doesnt Exit")
else:
    print("Table Exists")


# 
# #### **Read Data from Bronze Layer and perform silver level transformation!!**
# 

# In[3]:


# Define Schema for teh inflow CSVs
orderSchema = StructType([
    StructField("OrderNumber", StringType(), True),
    StructField("LineNumber", IntegerType(), True),
    StructField("OrderDate", DateType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Item", StringType(), True),
    StructField("Qty", IntegerType(), True),
    StructField("UnitPrice", FloatType(), True),
    StructField("Tax", FloatType(), True)
])


# In[4]:


# import all files to dataframe
df = spark.read.format("csv").option("header", "true").schema(orderSchema).load("Files/bronze/*.csv")
# Display the first 10 rows of the dataframe to preview your data
display(df.head(10))


# In[13]:


df = df.withColumn("SoureFile", input_file_name())\
    .withColumn("CreationTimeStamp", current_timestamp())\
    .withColumn("BucketSize",
         when(col('UnitPrice')> 1500, 'Big')
         .when((col('UnitPrice')> 800) & (col('UnitPrice')<= 1500), 'Medium')
         .otherwise('Small'))\
    .withColumn("CustomerName", expr("case when CustomerName is null or CustomerName == '' then 'Unknown' else CustomerName END AS CustomerName"))\
    .withColumn("update_status", lit(""))

display(df.filter(df['CustomerName'] != 'Unknown').show(5))


# In[6]:


## Create table by this option
# from pyspark.sql.types import *
# from delta.tables import *

# DeltaTable.createIfNotExists(spark) \
#     .tableName("sales.sales_silver") \
#     .addColumn("SalesOrderNumber", StringType()) \
#     .addColumn("SalesOrderLineNumber", IntegerType()) \
#     .addColumn("OrderDate", DateType()) \
#     .addColumn("CustomerName", StringType()) \
#     .addColumn("Email", StringType()) \
#     .addColumn("Item", StringType()) \
#     .addColumn("Quantity", IntegerType()) \
#     .addColumn("UnitPrice", FloatType()) \
#     .addColumn("Tax", FloatType()) \
#     .addColumn("FileName", StringType()) \
#     .addColumn("IsFlagged", BooleanType()) \
#     .addColumn("CreatedTS", DateType()) \
#     .addColumn("ModifiedTS", DateType()) \
#     .execute()


# In[17]:


# But i will use this option

# Check if silver table already exists

if not spark.catalog.tableExists('salesLakeHouse.sales_silver'):
    print("Table Doest Exists")
    df.write.mode('overwrite').format('delta').option("mergeSchema", "true").saveAsTable("salesLakeHouse.sales_silver")
else:
    print("Table Exists")


# In[8]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# -- Finding the grain level of transaction for update/merge
# select OrderNumber,CustomerName,OrderDate,Item, count(1) from salesLakeHouse.sales_silver 
# group by OrderNumber,CustomerName,OrderDate,Item
# having count(1)>1


# In[9]:


df.printSchema()


# ### **Now lets do a delta merge easy peasy**

# In[24]:


from delta.tables import *

# Update existing records and insert new ones based on a condition defined by the columns SalesOrderNumber and CustomerName

delta_target_tbl = DeltaTable.forPath(spark, 'Tables/sales_silver')
source_df = df


delta_target_tbl.alias('target')\
    .merge(
        source_df.alias('source'),
        """
        target.OrderNumber == source.OrderNumber 
        and 
        target.CustomerName == source.CustomerName
        and 
        target.OrderDate == source.OrderDate
        and 
        target.Item == source.Item
        """
    ).whenMatchedUpdate(
        set = {
            "OrderNumber": "source.OrderNumber",
            "LineNumber": "source.LineNumber",
            "OrderDate": "source.OrderDate",
            "CustomerName": "source.CustomerName",
            "Email": "source.Email",
            "Item": "source.Item",
            "Qty": "source.Qty",
            "UnitPrice": "source.UnitPrice",
            "Tax": "source.Tax",
            "SoureFile": "source.SoureFile",
            "CreationTimeStamp": "source.CreationTimeStamp",
            "BucketSize": "source.BucketSize",
            "update_status": lit("updated_record")
        }
    ).whenNotMatchedInsert(
        values={
            "OrderNumber": "source.OrderNumber",
            "LineNumber": "source.LineNumber",
            "OrderDate": "source.OrderDate",
            "CustomerName": "source.CustomerName",
            "Email": "source.Email",
            "Item": "source.Item",
            "Qty": "source.Qty",
            "UnitPrice": "source.UnitPrice",
            "Tax": "source.Tax",
            "SoureFile": "source.SoureFile",
            "CreationTimeStamp": "source.CreationTimeStamp",
            "BucketSize": "source.BucketSize"
        }
    ).execute()



# In[25]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT * from sales_silver

