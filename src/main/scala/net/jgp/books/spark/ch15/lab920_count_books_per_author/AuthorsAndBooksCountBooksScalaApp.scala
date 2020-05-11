package net.jgp.books.spark.ch15.lab920_count_books_per_author

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This lab combines a join with a group by to find out who published more
 * books among great authors.
 *
 * @author rambabu.posa
 */
object AuthorsAndBooksCountBooksScalaApp {

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
    val authorsDf: DataFrame = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    authorsDf.show
    authorsDf.printSchema

    filename = "data/books/books.csv"
    val booksDf = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)

    booksDf.show
    booksDf.printSchema

    var libraryDf = authorsDf
      .join(booksDf, authorsDf.col("id") === booksDf.col("authorId"), "left")
      .withColumn("bookId", booksDf.col("id"))
      .drop(booksDf.col("id"))
      .groupBy(authorsDf.col("id"), authorsDf.col("name"), authorsDf.col("link"))
      .count

    libraryDf = libraryDf.orderBy(libraryDf.col("count").desc)

    libraryDf.show
    libraryDf.printSchema

    spark.stop
  }

}
