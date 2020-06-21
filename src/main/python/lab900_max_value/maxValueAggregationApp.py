"""
  Max Value Aggregation

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
    path = "../../../../data/misc/"
    filename = "courses.csv"
    absolute_file_path = get_absolute_file_path(path, filename)

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

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Aggregates max values") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()
