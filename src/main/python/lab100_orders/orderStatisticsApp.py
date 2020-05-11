"""
  Orders analytics.

  @author rambabu.posa
"""
from pyspark.sql import (SparkSession, functions as F)
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/orders/orders.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Orders analytics") \
    .master("local[*]").getOrCreate()

# Reads a CSV file with header, called orders.csv,
# stores it in a dataframe
df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load(absolute_file_path)

# Calculating the orders info using the dataframe API
apiDf = df.groupBy(F.col("firstName"), F.col("lastName"), F.col("state")) \
    .agg(F.sum("quantity"), F.sum("revenue"), F.avg("revenue"))

apiDf.show(20)

# Calculating the orders info using SparkSQL
df.createOrReplaceTempView("orders")

sqlQuery = """
    SELECT firstName,lastName,state,SUM(quantity),SUM(revenue),AVG(revenue) 
    FROM orders 
    GROUP BY firstName, lastName, state
"""

sqlDf = spark.sql(sqlQuery)
sqlDf.show(20)

spark.stop()
