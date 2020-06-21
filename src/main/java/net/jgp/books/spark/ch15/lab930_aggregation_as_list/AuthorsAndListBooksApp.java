package net.jgp.books.spark.ch15.lab930_aggregation_as_list;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.struct;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This lab combines a join with a group by to find out the list of books by
 * author.
 * 
 * @author jgp
 */
public class AuthorsAndListBooksApp {

  public static void main(String[] args) {
    AuthorsAndListBooksApp app = new AuthorsAndListBooksApp();
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

    filename = "data/books/books.csv";
    Dataset<Row> booksDf = spark.read()
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(filename);

    Dataset<Row> libraryDf = authorsDf
        .join(
            booksDf,
            authorsDf.col("id").equalTo(booksDf.col("authorId")),
            "left")
        .withColumn("bookId", booksDf.col("id"))
        .drop(booksDf.col("id"))
        .orderBy(col("name").asc());
    libraryDf.show();
    libraryDf.printSchema();

    System.out.println("List of authors and their titles");
    Dataset<Row> booksByAuthorDf =
        libraryDf.groupBy(col("name")).agg(collect_list("title"));
    booksByAuthorDf.show(false);
    booksByAuthorDf.printSchema();

    System.out.println("List of authors and their titles, with ids");
    booksByAuthorDf = libraryDf
        .select("authorId", "name", "bookId", "title")
        .withColumn("book", struct(col("bookId"), col("title")))
        .drop("bookId", "title")
        .groupBy(col("authorId"), col("name"))
        .agg(collect_list("book").as("book"));
    booksByAuthorDf.show(false);
    booksByAuthorDf.printSchema();
  }
}
