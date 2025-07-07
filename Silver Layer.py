# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Script
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Access Using App

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.awstorageproject1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awstorageproject1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awstorageproject1.dfs.core.windows.net", "27a9631c-5bce-49b2-af16-d89cc3b1503d")
spark.conf.set("fs.azure.account.oauth2.client.secret.awstorageproject1.dfs.core.windows.net", "Dhj8Q~fNwpNArSOLKYLBkaTOKhMlqBrFgj17ZcKM")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awstorageproject1.dfs.core.windows.net", "https://login.microsoftonline.com/07ce8e71-6c69-4216-a617-08ac56233506/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Data
# MAGIC

# COMMAND ----------

df_cal=spark.read.format("csv").option("header","true").option("inferSchema","true").load('abfss://bronze@awstorageproject1.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

df_cus=spark.read.format("csv").option("header","true").option("inferSchema","true").load('abfss://bronze@awstorageproject1.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_prod_cat=spark.read.format("csv").option("header","true").option("inferSchema","true").load('abfss://bronze@awstorageproject1.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_prod_sub=spark.read.format("csv").option("header","true").option("inferSchema","true").load('abfss://bronze@awstorageproject1.dfs.core.windows.net/AdventureWorks_Product_Subcategories')

# COMMAND ----------

df_prod=spark.read.format("csv").option("header","true").option("inferSchema","true").load('abfss://bronze@awstorageproject1.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_return=spark.read.format("csv").option("header","true").option("inferSchema","true").load('abfss://bronze@awstorageproject1.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

df_sales=spark.read.format("csv").option("header","true").option("inferSchema","true").load('abfss://bronze@awstorageproject1.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

df_terri=spark.read.format("csv").option("header","true").option("inferSchema","true").load('abfss://bronze@awstorageproject1.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calender

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal=df.withColumn("Month",month(col('Date')))\
    .withColumn("Year",year(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstorageproject1.dfs.core.windows.net/AdventureWorks_Calendar")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Customers

# COMMAND ----------

df_cus.display()

# COMMAND ----------

# df_cus=df_cus.withColumn("FullName",concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName'))).display()

df_cus=df_cus.withColumn("FullName",concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))


# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstorageproject1.dfs.core.windows.net/AdventureWorks_Customers")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Product Category
# MAGIC

# COMMAND ----------

df_prod_cat.display()


# COMMAND ----------

df_prod_cat.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstorageproject1.dfs.core.windows.net/AdventureWorks_Product_Categories")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product SubCategory

# COMMAND ----------

df_prod_sub.display()

# COMMAND ----------

df_prod_sub.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstorageproject1.dfs.core.windows.net/AdventureWorks_Product_SubCategories")\
    .save()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Product

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod=df_prod.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
            .withColumn('ProductName',split(col('ProductName'), ' ')[0])

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstorageproject1.dfs.core.windows.net/AdventureWorks_Products")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Returns

# COMMAND ----------

df_return.display()


# COMMAND ----------

df_return.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstorageproject1.dfs.core.windows.net/AdventureWorks_Returns")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Territories

# COMMAND ----------

df_terri.display()


# COMMAND ----------

df_terri.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstorageproject1.dfs.core.windows.net/AdventureWorks_Territory")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Sales
# MAGIC

# COMMAND ----------

df_sales.display()


# COMMAND ----------

df_sales=df_sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales=df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales=df_sales.withColumn('Multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@awstorageproject1.dfs.core.windows.net/AdventureWorks_Sales")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_Order')).display()

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_terri.display()

# COMMAND ----------

