# Databricks notebook source
from pyspark.sql import Row

# Create data directly
data = [
    Row(name="Alice", age=25, city="New York"),
    Row(name="Bob", age=35, city="Los Angeles"),
    Row(name="Charlie", age=28, city="Chicago"),
    Row(name="David", age=42, city="New York"),
    Row(name="Eve", age=30, city="Los Angeles")
]

# Create DataFrame
df = spark.createDataFrame(data)

# Show data
df.show()

df.columns

# See schema (types of each column)
df.printSchema()

# Show top 5 rows
df.show(5)