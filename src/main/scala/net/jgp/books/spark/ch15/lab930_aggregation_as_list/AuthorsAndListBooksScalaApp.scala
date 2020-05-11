package net.jgp.books.spark.ch15.lab930_aggregation_as_list

import org.apache.spark.sql.functions.{col, collect_list, struct}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * This lab combines a join with a group by to find out the list of books by
 * author.
 *
 * @author rambabu.posa
 */
object AuthorsAndListBooksScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    val spark: SparkSession = SparkSession.builder
      .appName("Authors and Books")
      .master("local[*]")
      .getOrCreate

    var filename: String = "data/books/authors.csv"
    val authorsDf: Dataset[Row] = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    filename = "data/books/books.csv"
    val booksDf = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    val libraryDf = authorsDf
      .join(booksDf, authorsDf.col("id") === booksDf.col("authorId"), "left")
      .withColumn("bookId", booksDf.col("id"))
      .drop(booksDf.col("id"))
      .orderBy(col("name").asc)

    libraryDf.show()
    libraryDf.printSchema()

    println("List of authors and their titles")
    var booksByAuthorDf = libraryDf
      .groupBy(col("name"))
      .agg(collect_list("title"))

    booksByAuthorDf.show(false)
    booksByAuthorDf.printSchema()

    println("List of authors and their titles, with ids")
    booksByAuthorDf = libraryDf
      .select("authorId", "name", "bookId", "title")
      .withColumn("book", struct(col("bookId"), col("title")))
      .drop("bookId", "title")
      .groupBy(col("authorId"), col("name"))
      .agg(collect_list("book").as("book"))

    booksByAuthorDf.show(false)
    booksByAuthorDf.printSchema()

    spark.stop
  }

}
