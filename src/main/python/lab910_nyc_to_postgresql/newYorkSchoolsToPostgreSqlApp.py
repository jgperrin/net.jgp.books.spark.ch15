"""
  NYC schools analytics.

  @author rambabu.posa
"""
import os
import logging
import time
from pyspark.sql import (SparkSession, functions as F)
from pyspark.sql.types import (StructType, StructField, LongType,
                               StringType, IntegerType, DateType)

'''
 Returns absolute path.
'''
def get_absolute_path(filenames):
    current_dir = os.path.dirname(__file__)
    relative_path = f"../../../../data/nyc_school_attendance/{filenames}"
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

'''
 Loads a data file matching the 2018 format, then prepares it.
'''
def load_data_using_2018_format(spark, filenames):
    schema = StructType([StructField('schoolId', StringType(), False),
                         StructField('date', DateType(), False),
                         StructField('enrolled', IntegerType(), False),
                         StructField('present', IntegerType(), False),
                         StructField('absent', IntegerType(), False),
                         StructField('released', IntegerType(), False)])
    absolute_file_path = get_absolute_path(filenames)
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("dateFormat", "yyyyMMdd") \
        .schema(schema) \
        .load(absolute_file_path)
    df = df.withColumn("schoolYear", F.lit(2018))
    return df

'''
 Common loader for most datasets, accepts a date format 
 as part of the parameters.
'''
def load_data(spark, filenames, dateformat):
    schema = StructType([StructField('schoolId', StringType(), False),
                         StructField('date', DateType(), False),
                         StructField('schoolYear', StringType(), False),
                         StructField('enrolled', IntegerType(), False),
                         StructField('present', IntegerType(), False),
                         StructField('absent', IntegerType(), False),
                         StructField('released', IntegerType(), False)])
    absolute_file_path = get_absolute_path(filenames)
    df = spark.read.format("csv") \
        .option("header", True) \
        .option("dateFormat", dateformat) \
        .schema(schema) \
        .load(absolute_file_path)
    df = df.withColumn("schoolYear", F.substring(F.col("schoolYear"), 1, 4))
    return df

"""
  Load a data file matching the 2006 format.
"""
def load_data_using_2006_format(spark, filenames):
    return load_data(spark,filenames, "yyyyMMdd")

"""
  Load a data file matching the 2015 format.
"""
def load_data_using_2015_format(spark, filenames):
    return load_data(spark, filenames, "MM/dd/yyyy")


def main(spark):
    format2018df = load_data_using_2018_format(spark,"2018*.csv")

    format2015df = load_data_using_2015_format(spark,"2015*.csv")

    format2006df = load_data_using_2006_format(spark,"200*.csv")

    format2006df2 = load_data_using_2006_format(spark,"2012*.csv")

    df = format2018df.unionByName(format2015df).unionByName(format2006df).unionByName(format2006df2)

    t2 = int(round(time.time() * 1000))

    # Properties to connect to the database,
    # the JDBC driver is part of our build.sbt
    dbConnectionUrl = "jdbc:postgresql://localhost/spark_labs"
    mode = "overwrite"
    table = "ch13_nyc_schools"
    properties = {"user": "postgres","password": "password","driver": "org.postgresql.Driver"}
    # Write in a table called ch02
    df.write.jdbc(url=dbConnectionUrl, table=table, mode=mode, properties=properties)

    t3 = int(round(time.time() * 1000))

    logging.info(f"Dataset contains {df.count} rows, processed in {t3 - t0} ms.")
    logging.info(f"Spark init ... {t1 - t0} ms.")
    logging.info(f"Ingestion .... {t2 - t1} ms.")
    logging.info(f"Output ....... {t3 - t2} ms.")

    df.sample(.5).show(5)
    df.printSchema()


if __name__ == "__main__":
    t0 = int(round(time.time() * 1000))

    # Creates a session on a local master
    spark = SparkSession.builder.appName("NYC schools to PostgreSQL") \
        .master("local[*]").getOrCreate()

    t1 = int(round(time.time() * 1000))

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()
