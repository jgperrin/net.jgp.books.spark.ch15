package net.jgp.books.spark.ch15.lab400_udaf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{callUDF, col, sum, when}

/**
 * Orders analytics.
 *
 * @author rambabu.posa
 */
object PointsPerOrderScalaApp {

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
      .appName("Orders loyalty point")
      .master("local[*]")
      .getOrCreate

    val pointsUdf = new PointAttributionScalaUdaf
    spark.udf.register("pointAttribution", pointsUdf)

    // Reads a CSV file with header, called orders.csv, stores it in a
    // dataframe
    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("data/orders/orders.csv")

    // Calculating the points for each customer, not each order
    val pointDf = df
      .groupBy(col("firstName"), col("lastName"), col("state"))
      .agg(sum("quantity"), callUDF("pointAttribution", col("quantity")).as("point"))

    pointDf.show(20)

    // Alternate way: calculate order by order
    val max = PointAttributionUdaf.MAX_POINT_PER_ORDER
    val eachOrderDf = df
      .withColumn("point", when(col("quantity").$greater(max), max).otherwise(col("quantity")))
      .groupBy(col("firstName"), col("lastName"), col("state"))
      .agg(sum("quantity"), sum("point").as("point"))

    eachOrderDf.show(20)

    spark.stop
  }
}
