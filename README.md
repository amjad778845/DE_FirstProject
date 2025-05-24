# DE_FirstProject
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
#3.Write the sales_fact table to a Parquet file, read it back, and perform the required transformation queries
