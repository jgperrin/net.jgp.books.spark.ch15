package net.jgp.books.spark.ch15.lab100_orders

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum}

/**
 * Orders analytics.
 *
 * @author rambabu.posa
 */
object OrderStatisticsScalaApp {

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
    val spark: SparkSession = SparkSession.builder
      .appName("Orders analytics")
      .master("local[*]")
      .getOrCreate

    // Reads a CSV file with header, called orders.csv, stores it in a
    // dataframe
    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("data/orders/orders.csv")

    // Calculating the orders info using the dataframe API
    val apiDf = df
      .groupBy(col("firstName"), col("lastName"), col("state"))
      .agg(sum("quantity"), sum("revenue"), avg("revenue"))

    apiDf.show(20)

    // Calculating the orders info using SparkSQL
    df.createOrReplaceTempView("orders")

    val sqlQuery = "SELECT firstName,lastName,state,SUM(quantity),SUM(revenue),AVG(revenue) " +
      "FROM orders " +
      "GROUP BY firstName, lastName, state"

    val sqlDf = spark.sql(sqlQuery)
    sqlDf.show(20)

    spark.stop
  }

}
