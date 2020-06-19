"""
  NYC schools analytics.

  @author rambabu.posa
"""
import os
import logging
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

    master_df = format2018df.unionByName(format2015df).unionByName(format2006df).unionByName(format2006df2)

    master_df = master_df.cache()

    # Shows at most 5 rows from the dataframe - this is the dataframe we
    # can use to build our aggregations on
    logging.debug(f"Dataset contains {master_df.count()} rows")

    master_df.sample(.5).show(5)
    master_df.printSchema()

    # Unique schools
    unique_schools_df = master_df.select("schoolId").distinct()

    logging.debug(f"Dataset contains {unique_schools_df.count()} unique schools")

    # Calculating the average enrollment for each school
    average_enrollment_df = master_df.groupBy(F.col("schoolId"), F.col("schoolYear")) \
        .avg("enrolled", "present", "absent") \
        .orderBy("schoolId", "schoolYear")

    logging.info("Average enrollment for each school")

    average_enrollment_df.show(20)

    # Evolution of # of students in the schools
    student_count_peryear_df = average_enrollment_df.withColumnRenamed("avg(enrolled)", "enrolled") \
        .groupBy(F.col("schoolYear")) \
        .agg(F.sum("enrolled").alias("enrolled")) \
        .withColumn("enrolled", F.floor("enrolled").cast(LongType())) \
        .orderBy("schoolYear")

    logging.info("Evolution of # of students per year")

    student_count_peryear_df.show(20)

    maxStudentRow = student_count_peryear_df.orderBy(F.col("enrolled").desc()).first()

    year = maxStudentRow[0]
    max = maxStudentRow[1]

    logging.debug(f"{year} was the year with most students, the district served {max} students.")

    # Evolution of # of students in the schools
    relative_student_count_peryear_df = student_count_peryear_df.withColumn("max", F.lit(max))

    relative_student_count_peryear_df = relative_student_count_peryear_df \
        .withColumn("delta", F.expr("max - enrolled")) \
        .drop("max") \
        .orderBy("schoolYear")

    logging.info(f"Variation on the enrollment from {year}:")
    relative_student_count_peryear_df.show(20)

    # Most enrolled per school for each year
    max_enrolled_perschool_df = master_df.groupBy(F.col("schoolId"), F.col("schoolYear")) \
        .max("enrolled") \
        .orderBy("schoolId", "schoolYear")

    logging.info("Maximum enrollement per school and year")
    max_enrolled_perschool_df.show(20)

    # Min absent per school for each year
    min_absentee_df = master_df.groupBy(F.col("schoolId"), F.col("schoolYear")) \
        .min("absent") \
        .orderBy("schoolId", "schoolYear")

    logging.info("Minimum absenteeism per school and year")
    min_absentee_df.show(20)

    # Min absent per school for each year, as a % of enrolled
    absenteeRatioDf = master_df.groupBy(F.col("schoolId"), F.col("schoolYear")) \
        .agg(F.max("enrolled").alias("enrolled"), F.avg("absent").alias("absent"))

    absenteeRatioDf = absenteeRatioDf.groupBy(F.col("schoolId")) \
        .agg(F.avg("enrolled").alias("avg_enrolled"), F.avg("absent").alias("avg_absent")) \
        .withColumn("%", F.expr("avg_absent / avg_enrolled * 100")) \
        .filter(F.expr("avg_enrolled > 10")) \
        .orderBy("%", "avg_enrolled")

    logging.info("Schools with the least absenteeism")
    absenteeRatioDf.show(5)

    logging.info("Schools with the most absenteeism")
    absenteeRatioDf.orderBy(F.col("%").desc()).show(5)

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("NYC schools analytics") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()
