"""
  This lab combines a join with a group by to find out who published more
  books among great authors.

  @author rambabu.posa
"""
import os
from pyspark.sql import (SparkSession, functions as F)

'''
 Returns absolute path.
'''
def get_absolute_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = f"{path}{filename}"
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path


def main(spark):
    path = "../../../../data/books/"
    filename = "authors.csv"
    authors_filename = get_absolute_path(path, filename)

    authorsDf = spark.read.format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load(authors_filename)

    authorsDf.show()
    authorsDf.printSchema()

    filename = "books.csv"
    books_filename = get_absolute_path(path, filename)

    booksDf = spark.read.format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load(books_filename)

    booksDf.show()
    booksDf.printSchema()

    libraryDf = authorsDf.join(booksDf, authorsDf.id == booksDf.authorId, "left") \
        .withColumn("bookId", booksDf.id) \
        .drop(booksDf.id) \
        .groupBy(authorsDf.id, authorsDf.name, authorsDf.link) \
        .count()

    libraryDf = libraryDf.orderBy(F.col("count").desc())

    libraryDf.show()
    libraryDf.printSchema()

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Authors and Books") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()