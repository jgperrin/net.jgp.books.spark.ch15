package net.jgp.books.spark.ch15.lab920_count_books_per_author;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This lab combines a join with a group by to find out who published more
 * books among great authors.
 * 
 * @author jgp
 */
public class AuthorsAndBooksCountBooksApp {

  public static void main(String[] args) {
    AuthorsAndBooksCountBooksApp app = new AuthorsAndBooksCountBooksApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Authors and Books")
        .master("local")
        .getOrCreate();

    String filename = "data/books/authors.csv";
    Dataset<Row> authorsDf = spark.read()
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(filename);
    authorsDf.show();
    authorsDf.printSchema();

    filename = "data/books/books.csv";
    Dataset<Row> booksDf = spark.read()
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(filename);
    booksDf.show();
    booksDf.printSchema();

    Dataset<Row> libraryDf = authorsDf
        .join(
            booksDf,
            authorsDf.col("id").equalTo(booksDf.col("authorId")),
            "left")
        .withColumn("bookId", booksDf.col("id"))
        .drop(booksDf.col("id"))
        .groupBy(
            authorsDf.col("id"),
            authorsDf.col("name"),
            authorsDf.col("link"))
        .count();

    libraryDf = libraryDf.orderBy(libraryDf.col("count").desc());

    libraryDf.show();
    libraryDf.printSchema();
  }
}
