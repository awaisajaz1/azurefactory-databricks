#!/usr/bin/env python
# coding: utf-8

# ## GoldCuration
# 
# New notebook

# In[1]:


from pyspark.sql.functions import *
from pyspark.sql.types import *


# In[7]:


## Read Silver Transformed Table and start creating Dimension from that

# df = spark.read.table("salesLakeHouse.sales_silver")
df = spark.sql("SELECT * from salesLakeHouse.sales_silver")
display(df.show())


# ### Date Dimension

# In[5]:


from delta.tables import *

DeltaTable.createIfNotExists(spark)\
.tableName("salesLakeHouse.date_dim_gold")\
.addColumn("DateKey", DateType(), False)\
.addColumn("Day", IntegerType(), False)\
.addColumn("Month", IntegerType(), False)\
.addColumn("Year", IntegerType(), False)\
.addColumn("mmmyyyy", StringType(), False)\
.addColumn("yyyymm", StringType(), False)\
.execute()


# ## Customer Dimension

# In[40]:


# Create customer_gold dimension delta table
DeltaTable.createIfNotExists(spark) \
    .tableName("salesLakeHouse.customer_dim_gold") \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email",  StringType()) \
    .addColumn("FirstName", StringType()) \
    .addColumn("LastName", StringType()) \
    .addColumn("CustomerID", LongType()) \
    .execute()


# In[13]:


## Load Data in Date Dataframe

date_df = df.dropDuplicates(['OrderDate'])

date_df = date_df.select(
    col('OrderDate').alias('DateKey'),
    dayofmonth('OrderDate').alias('Day'),
    month('OrderDate').alias('Month'),
    year('OrderDate').alias('Year'),
    date_format(col('OrderDate'), "MMM-yyyy").alias('mmmyyyy'),
    date_format(col('OrderDate'),"yyyy-MM").alias('yyyymm')
)

display(date_df.show())


# In[18]:


from delta.tables import *

deltaTable = DeltaTable.forName(spark, "salesLakeHouse.date_dim_gold")

date_source = date_df

deltaTable.alias("gold")\
    .merge(
        date_source.alias("silver"),
        "gold.DateKey = silver.DateKey"
).whenMatchedUpdate(
    set = {

    }
).whenNotMatchedInsert(
    values={
        "DateKey": "silver.DateKey",
       "Day": "silver.Day",
       "Month": "silver.Month",
       "Year": "silver.Year",
       "mmmyyyy": "silver.mmmyyyy",
       "yyyymm": "silver.yyyymm"
    }
).execute()


# In[ ]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# -- SELECT * from salesLakeHouse.date_dim_gold


# In[23]:


## Load Data in Customer Dataframe

cust_df = df.dropDuplicates(["CustomerName","Email"]).select("CustomerName", "Email")\
.withColumn("FirstName", split(("CustomerName"), " ").getItem(0))\
.withColumn("LastName", split(("CustomerName"), " ").getItem(1))

display(cust_df.show(5))


# ## Create Customer Key

# In[32]:


from pyspark.sql.functions import monotonically_increasing_id, col, lit, max, coalesce

max_cust_id = spark.sql("select coalesce(max(CustomerID),0) CustomerID  from salesLakeHouse.customer_dim_gold").first()[0] + 1
print(max_cust_id)

## We shall use left anti join which equilent to left join where right table has null entries and we use pushdown predicates <> null
dfdimCustomer = spark.read.table("salesLakeHouse.customer_dim_gold")
customer_curate = cust_df.join(dfdimCustomer, (cust_df.CustomerName == dfdimCustomer.CustomerName) & (cust_df.Email == dfdimCustomer.Email), 'left_anti')\
    .withColumn("CustomerID", monotonically_increasing_id() + max_cust_id )

display(customer_curate.show(5))


# In[41]:


from delta.tables import *

deltaTable = DeltaTable.forName(spark, "salesLakeHouse.customer_dim_gold")

deltaTable.alias("gold")\
    .merge(
        customer_curate.alias("updates"),
        "gold.CustomerName = updates.CustomerName AND gold.Email = updates.Email"
    ).whenMatchedUpdate(
        set = {

        }
    ).whenNotMatchedInsert(
        values = {
            "CustomerName": "updates.CustomerName",
            "Email": "updates.Email",
            "FirstName": "updates.FirstName",
            "LastName": "updates.LastName",
            "CustomerID": "updates.CustomerID"
        }
    ).execute()


# In[60]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT * from salesLakeHouse.customer_dim_gold 


# In[64]:


# Check if silver table already exists
from delta.tables import *

customer_dim = spark.read.table("salesLakeHouse.customer_dim_gold")
date_dim = spark.read.table("salesLakeHouse.date_dim_gold")

df_goldfact = df.alias("df").join(customer_dim.alias("customer_dim"), (df.CustomerName==customer_dim.CustomerName) & (df.Email==customer_dim.Email), 'left')\
        .join(date_dim.alias("date_dim"), (df.OrderDate==date_dim.DateKey), 'left')\
        .select(\
            col("customer_dim.CustomerID"),\
            col("date_dim.DateKey"),\
            col("df.Qty").alias("Quantity"),\
            col("df.UnitPrice"), \
            col("df.Tax") \
        )



if not spark.catalog.tableExists('salesLakeHouse.orderfact_gold'):

    df_goldfact.write.mode("overwrite").format("delta").saveAsTable("salesLakeHouse.orderfact_gold")


else:
    deltaTable = DeltaTable.forName(spark, "salesLakeHouse.orderfact_gold")

    deltaTable.alias("gold")\
        .merge(
            df_goldfact.alias("updates"),
            'gold.DateKey = updates.DateKey AND gold.CustomerID = updates.CustomerID'
        ).whenMatchedUpdate(
            set = {

            }
        ).whenNotMatchedInsert(
            values = {
                "CustomerID": "updates.CustomerID",
                "DateKey": "updates.DateKey",
                "Quantity": "updates.Quantity",
                "UnitPrice": "updates.UnitPrice",
                "Tax": "updates.Tax"
            }
        ).execute()


# In[65]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# -- DELETE FROM salesLakeHouse.orderfact_gold WHERE 1=1;
# SELECT * from salesLakeHouse.orderfact_gold

