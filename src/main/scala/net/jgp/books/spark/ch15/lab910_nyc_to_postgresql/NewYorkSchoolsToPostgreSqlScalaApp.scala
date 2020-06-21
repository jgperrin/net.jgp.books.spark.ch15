package net.jgp.books.spark.ch15.lab910_nyc_to_postgresql

import java.util.Properties

import org.apache.spark.sql.functions.{col, lit, substring}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * NYC schools analytics.
 *
 * @author rambabu.posa
 */
class NewYorkSchoolsToPostgreSqlScalaApp {
  private val log = LoggerFactory.getLogger(classOf[NewYorkSchoolsToPostgreSqlScalaApp])

  /**
   * The processing code.
   */
  def start(): Unit = {
    val t0: Long = System.currentTimeMillis

    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("NYC schools to PostgreSQL")
      .master("local[*]")
      .getOrCreate

    val t1 = System.currentTimeMillis

    var df = loadDataUsing2018Format(spark,
      Seq("data/nyc_school_attendance/2018*.csv"))

    df = df.unionByName(loadDataUsing2015Format(spark,
      Seq("data/nyc_school_attendance/2015*.csv")))

    df = df.unionByName(loadDataUsing2006Format(spark,
      Seq("data/nyc_school_attendance/200*.csv", "data/nyc_school_attendance/2012*.csv")))

    val t2 = System.currentTimeMillis

    val dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs"

    // Properties to connect to the database,
    // the JDBC driver is part of our build.sbt
    val prop = new Properties
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", "postgres")
    prop.setProperty("password", "password")

    // Write in a table called ch02
    df.write.mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "ch13_nyc_schools", prop)
    val t3 = System.currentTimeMillis

    log.info(s"Dataset contains ${df.count} rows, processed in ${t3 - t0} ms.")
    log.info(s"Spark init ... ${t1 - t0} ms.")
    log.info(s"Ingestion .... ${t2 - t1} ms.")
    log.info(s"Output ....... ${t3 - t2} ms.")

    df.sample(.5).show(5)
    df.printSchema()

    spark.stop
  }

  /**
   * Loads a data file matching the 2018 format, then prepares it.
   *
   * @param fileNames
   * @return
   */
  private def loadDataUsing2018Format(spark: SparkSession, fileNames: Seq[String]): DataFrame = {
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("schoolId", DataTypes.StringType, false),
      DataTypes.createStructField("date", DataTypes.DateType, false),
      DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
      DataTypes.createStructField("present", DataTypes.IntegerType, false),
      DataTypes.createStructField("absent", DataTypes.IntegerType, false),
      DataTypes.createStructField("released", DataTypes.IntegerType, false)))

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("dateFormat", "yyyyMMdd")
      .schema(schema)
      .load(fileNames: _*)

    df.withColumn("schoolYear", lit(2018))
  }

  /**
   * Load a data file matching the 2006 format.
   *
   * @param fileNames
   * @return
   */
  private def loadDataUsing2006Format(spark: SparkSession, fileNames: Seq[String]): DataFrame =
    loadData(spark, fileNames, "yyyyMMdd")

  /**
   * Load a data file matching the 2015 format.
   *
   * @param fileNames
   * @return
   */
  private def loadDataUsing2015Format(spark: SparkSession, fileNames: Seq[String]): DataFrame =
    loadData(spark, fileNames, "MM/dd/yyyy")

  /**
   * Common loader for most datasets, accepts a date format as part of the
   * parameters.
   *
   * @param fileNames
   * @param dateFormat
   * @return
   */
  private def loadData(spark: SparkSession, fileNames: Seq[String], dateFormat: String): DataFrame = {
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("schoolId", DataTypes.StringType, false),
      DataTypes.createStructField("date", DataTypes.DateType, false),
      DataTypes.createStructField("schoolYear", DataTypes.StringType, false),
      DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
      DataTypes.createStructField("present", DataTypes.IntegerType, false),
      DataTypes.createStructField("absent", DataTypes.IntegerType, false),
      DataTypes.createStructField("released", DataTypes.IntegerType, false)))

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("dateFormat", dateFormat)
      .schema(schema)
      .load(fileNames: _*)

    df.withColumn("schoolYear", substring(col("schoolYear"), 1, 4))
  }

}

object NewYorkSchoolsToPostgreSqlScalaApplication {
  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val app = new NewYorkSchoolsToPostgreSqlScalaApp
    app.start
  }

}
