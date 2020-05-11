package net.jgp.books.spark.ch15.lab900_max_value

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

/**
 * Max Value Aggregation
 *
 * @author rambabu.posa
 */
object MaxValueAggregationScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Aggregates max values")
      .master("local[*]")
      .getOrCreate

    // Reads a CSV file with header, called courses.csv, stores it in a
    // dataframe
    val rawDf = spark.read
      .format("csv")
      .option("header", true)
      .option("sep", "|")
      .load("data/misc/courses.csv")

    // Shows at most 20 rows from the dataframe
    rawDf.show(20)

    // Performs the aggregation, grouping on columns id, batch_id, and
    // session_name
    val maxValuesDf = rawDf
      .select("*")
      .groupBy(col("id"), col("batch_id"), col("session_name"))
      .agg(max("time"))

    maxValuesDf.show(5)

    spark.stop
  }

}
