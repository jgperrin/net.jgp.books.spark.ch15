"""
  Orders analytics.

  @author rambabu.posa
"""
import os
from pyspark.sql import (SparkSession, functions as F)

def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = f"{path}{filename}"
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

def main(spark):
    path = "../../../../data/orders/"
    filename = "orders.csv"
    absolute_file_path = get_absolute_file_path(path, filename)
    # Reads a CSV file with header, called orders.csv,
    # stores it in a dataframe
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load(absolute_file_path)

    # Calculating the orders info using the dataframe API
    api_df = df.groupBy(F.col("firstName"), F.col("lastName"), F.col("state")) \
        .agg(F.sum("quantity"), F.sum("revenue"), F.avg("revenue"))

    api_df.show(20)

    # Calculating the orders info using SparkSQL
    df.createOrReplaceTempView("orders")

    sql_query = """
        SELECT firstName,lastName,state,SUM(quantity),SUM(revenue),AVG(revenue) 
        FROM orders 
        GROUP BY firstName, lastName, state
    """
    sql_df = spark.sql(sql_query)
    sql_df.show(20)

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Orders analytics") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()
