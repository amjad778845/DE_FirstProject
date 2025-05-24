# Databricks notebook source
# DBTITLE 1,Project Description
#In this project, I am working with the following tables:
#1.product_dimension
#2.manufacturer_dimension
#3.customer_dimension
#4.sales (fact table)
#I aim to answer the following five transformation questions:
#1.What is the total revenue of products sold in a specific category?
#2.What is the average price of products with a specific property?
#3.Which product has the highest price?
#4.What is the profit margin of each product?
#5.How can we group products by manufacturer and calculate their total sales?
#To address these questions, I will create a sales_fact table and perform the necessary transformations.
#Steps Involved in Implementing the Project:
#1.Read all four CSV files and clean the data.
#2.Use join operations to merge the columns needed for the sales_fact table and calculate metrics like total cost, revenue, etc.
#3.Write the sales_fact table to a Parquet file, read it back, and perform the required transformation queries.


# COMMAND ----------

# DBTITLE 1,Import libraries for Pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,DateType
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Assign filePath
product_filePath = "/FileStore/product_table.csv"
manufacture_filePath = "/FileStore/manufacturer_table.csv"
customer_filePath = "/FileStore/customer_table.csv"
sales_filePath = "/FileStore/sales_fact_table.csv"

# COMMAND ----------

# DBTITLE 1,Creating Schema for all the tables
# For Best practices I am creating schema for all the given table but we can use InferSchema = True it will read the schema of the table but for good practices I am creating schema

product_Schema = StructType([
    StructField("product_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("category",StringType(),True),
    StructField("Warranty",IntegerType(),True),
    StructField("price",DoubleType(),True),
    StructField("manufacturer_id",IntegerType(),True)

])

manufactur_Schema = StructType([
    StructField("manufacturer_id",IntegerType(),True),
    StructField("manufacturer_name",StringType(),True),
    StructField("country",StringType(),True),
    StructField("established_year",IntegerType(),True)
])

customer_Schema = StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("customer_name",StringType(),True),
    StructField("emai",StringType(),True),
    StructField("phone",StringType(),True),
    StructField("city",StringType(),True),
    StructField("registration_date",StringType(),True)
])

sales_Schema = StructType([
    StructField("sales_id",IntegerType(),True),
    StructField("customer_id",IntegerType(),True),
    StructField("product_id",IntegerType(),True),
    StructField("date",StringType(),True),
    StructField("quantity",IntegerType(),True),
    StructField("discount",DoubleType(),True)
])


# COMMAND ----------

# DBTITLE 1,Reading the table and Storing it into dataframe variable
df_sale = spark.read.csv(sales_filePath,header=True,schema = sales_Schema)
df_product = spark.read.csv(product_filePath,header=True,schema=product_Schema)
df_munufacture = spark.read.csv(manufacture_filePath,header=True,schema=manufactur_Schema)
df_customer = spark.read.csv(customer_filePath,header=True,schema=customer_Schema)

# COMMAND ----------

# DBTITLE 1,Displaying the all table
df_sale.show(5)
df_product.show(5)
df_munufacture.show(5)
df_customer.show(5)

# COMMAND ----------

# DBTITLE 1,Printing schema to see the date column data type
df_sale.printSchema()

# COMMAND ----------

# DBTITLE 1,Converting the data type for Date column into mm-dd-yyyy
df_sale = df_sale.withColumn("date_parsed", to_date("date", "dd/MM/yyyy"))
df_sale = df_sale.withColumn("date_formatted", date_format("date_parsed", "dd-MM-yyyy"))
df_sale.show()

# COMMAND ----------

df_sale = df_sale.drop("date").withColumnRenamed("date_formatted", "date")

df_sale.show()

# COMMAND ----------

df_customer.printSchema()

# COMMAND ----------



df_customer = df_customer.withColumn("date_parsed", to_date("registration_date", "yyyy-MM-dd"))

# Then format as needed, e.g., "dd-MM-yyyy"
df_customer = df_customer.withColumn("date_formatted", date_format("date_parsed", "dd-MM-yyyy"))
display(df_customer)

# COMMAND ----------

df_customer = df_customer .drop("registration_date").withColumnRenamed("date_formatted", "registration_date")

df_customer .show()

# COMMAND ----------

# DBTITLE 1,Data cleaning for Customer table
#  Count total nulls in the whole DataFrame

null_counts = df_customer.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_customer.columns
])

null_counts.show()



# COMMAND ----------

# DBTITLE 1,Dropping the row where it have null values in customer table
df_customer = df_customer.dropna()

# COMMAND ----------

null_counts = df_customer.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_customer.columns
])

null_counts.show()


# COMMAND ----------

# DBTITLE 1,Deleting Phone no column in customer table
df_customer = df_customer.drop("phone")
display(df_customer)

# COMMAND ----------

# DBTITLE 1,Cleaning Product table
display(df_product)

# COMMAND ----------

productTable_nullCount = df_product.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_product.columns
])

productTable_nullCount.show()

# COMMAND ----------

df_product= df_product.dropna()

# COMMAND ----------

productTable_nullCount = df_product.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_product.columns
])

productTable_nullCount.show()

# COMMAND ----------

display(df_product)

# COMMAND ----------

# DBTITLE 1,Cleaning of Munufature table
manufuter_null = df_munufacture.select([
    sum(when(col(c).isNull(),1).otherwise(0)).alias(c)
    for c in df_munufacture.columns
    ])
manufuter_null.show()


# COMMAND ----------

df_munufacture=df_munufacture.dropna()

# COMMAND ----------

manufuter_null = df_munufacture.select([
    sum(when(col(c).isNull(),1).otherwise(0)).alias(c)
    for c in df_munufacture.columns
    ])
manufuter_null.show()

# COMMAND ----------

# DBTITLE 1,cleaning of Sale table
sales_nullCount = df_sale.select(
    [
        sum(when(col(c).isNull(),1).otherwise(0)).alias(c)
        for c in df_sale.columns
    ]
)

sales_nullCount.show()

# COMMAND ----------

df_sale.show(2)
df_customer.show(2)
df_product.show(2)
df_munufacture.show(2)

# COMMAND ----------

# DBTITLE 1,Creating sales fact table
# As we can see the above data does not contains the proper details we will join with other table to find to add the column in fact sales table

df_sales_factTable = df_sale.join(df_product,df_sale["product_id"] == df_product["product_id"],"inner").select(
    df_sale["sales_id"],
    df_sale["customer_id"],
    df_product["product_id"],
    df_product["manufacturer_id"],
    df_sale["date"],
    df_sale["quantity"],
    df_product["Warranty"],
    df_sale["discount"],
    df_product["price"]    
)

df_sales_factTable.show(5)


# COMMAND ----------

 df_sales_factTable = df_sales_factTable.withColumn("total_price",round(col("quantity")*col("price"),2)).select("*")
 df_sales_factTable = df_sales_factTable.withColumn("price_after_discount", round(col("total_price")*(1-col("discount"))    ,2))
 display(df_sales_factTable)

# COMMAND ----------

# DBTITLE 1,Transformation Question
#1.What is the total revenue of products sold in a specific category?

df_categoryRevenue = df_sales_factTable.join(df_product,df_sales_factTable["product_id"]==df_product["product_id"],"inner")\
    .groupBy("category")\
        .agg(
            round(sum(col("price_after_discount")),2).alias("category_revenue")
        ).select("category","category_revenue")
df_categoryRevenue.show()

# COMMAND ----------

#3.Which product has the highest price?
max_price = df_sales_factTable.agg(max("price").alias("Max_price")).collect()[0]["Max_price"]

df_highestPrice_product = df_sales_factTable.filter(df_sales_factTable["price"]== max_price).select(df_sales_factTable["product_id"],df_sales_factTable["price"])

df_highestPrice_product.show()

# COMMAND ----------

