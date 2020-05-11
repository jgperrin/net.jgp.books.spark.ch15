"""
  Max Value Aggregation

  @author rambabu.posa
"""
import os
from pyspark.sql import (SparkSession, functions as F)

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/misc/courses.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Aggregates max values") \
    .master("local[*]").getOrCreate()

# Reads a CSV file with header, called courses.csv,
# stores it in a dataframe
raw_df = spark.read.format("csv") \
    .option("header", True) \
    .option("sep", "|") \
    .load(absolute_file_path)

# Shows at most 20 rows from the dataframe
raw_df.show(20)

# Performs the aggregation, grouping on columns id, batch_id, and
# session_name
max_values_df = raw_df.select("*") \
    .groupBy(F.col("id"), F.col("batch_id"), F.col("session_name")) \
    .agg(F.max("time"))

max_values_df.show(5)

spark.stop()
