"""
  This lab combines a join with a group by to find out
  the list of books by author.

  @author rambabu.posa
"""
import os
import logging
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

    filename = "books.csv"
    books_filename = get_absolute_path(path, filename)
    booksDf = spark.read.format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load(books_filename)

    libraryDf = authorsDf.join(booksDf, authorsDf["id"] == booksDf["authorId"], "left") \
        .withColumn("bookId", booksDf["id"]) \
        .drop(booksDf["id"]) \
        .orderBy(authorsDf["name"].asc())

    libraryDf.show()
    libraryDf.printSchema()

    logging.warn("List of authors and their titles")

    booksByAuthorDf = libraryDf.groupBy(F.col("name")) \
        .agg(F.collect_list("title"))

    booksByAuthorDf.show(truncate=False)
    booksByAuthorDf.printSchema()

    logging.warn("List of authors and their titles, with ids")

    booksByAuthorDf = libraryDf.select("authorId", "name", "bookId", "title") \
        .withColumn("book", F.struct(F.col("bookId"), F.col("title"))) \
        .drop("bookId", "title") \
        .groupBy("authorId", "name") \
        .agg(F.collect_list("book").alias("book"))

    booksByAuthorDf.show(truncate=False)
    booksByAuthorDf.printSchema()

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Authors and Books") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()