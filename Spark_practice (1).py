# Databricks notebook source
# Step 1: Customers table
customers = [
    ("Alice", 25, "New York"),
    ("Bob", 35, "Los Angeles"),
    ("Charlie", 28, "Chicago"),
    ("David", 42, "New York"),
    ("Eve", 30, "Los Angeles")
]
df_customers = spark.createDataFrame(customers, ["name", "age", "city"])
df_customers.show()

# Step 2: Orders table
orders = [
    (1, "Alice", "Laptop", 1200),
    (2, "Bob", "Phone", 800),
    (3, "Alice", "Mouse", 40),
    (4, "David", "Keyboard", 60),
    (5, "Grace", "Monitor", 200)
]
df_orders = spark.createDataFrame(orders, ["order_id", "name", "product", "amount"])
df_orders.show()

# Step 3: Inner join
df_inner = df_customers.join(df_orders, on="name", how="inner")
df_inner.show()