The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean-Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).

---

# Spark in Action, 2nd edition - chapter 15

Welcome to Spark in Action, 2e, chapter 15. This chapter is about **aggregates**.

This code is designed to work with Apache Spark v3.0.1.

## Datasets

Datasets can be downloaded from:
* Historical daily attendance by school from 2006 to now, from the [open data platform from the city of New York][1] at [https://data.cityofnewyork.us][1].
  + [2018-2019](https://data.cityofnewyork.us/Education/2018-2019-Daily-Attendance/x3bb-kg5j)
  + [2015-2018](https://data.cityofnewyork.us/Education/2015-2018-Historical-Daily-Attendance-By-School/3y5p-x48f)
  + [2012-2015](https://data.cityofnewyork.us/Education/2012-2015-Historical-Daily-Attendance-By-School/pffu-gbfi)
  + [2009-2012](https://data.cityofnewyork.us/Education/2009-2012-Historical-Daily-Attendance-By-School/wpqj-3buw)
  + [2006-2009](https://data.cityofnewyork.us/Education/2006-2009-Historical-Daily-Attendance-By-School/xwxx-rnki)
  
## Labs

Each chapter has one or more labs. Labs are examples used for teaching in the book(https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). You are encouraged to take ownership of the code and modify it, experiment with it, hence the use of the term **lab**.

### Lab \#100

The `OrderStatisticsApp` application does the following:

1.	It acquires a session (a `SparkSession`).
2.	It asks Spark to load (ingest) a dataset in CSV format.
3.	Spark stores the contents in a dataframe, do some transformations to observe Orders analytics.

### Lab \#300

TBD

### Lab \#400

TBD

### Lab \#900

TBD

### Lab \#910

TBD

### Lab \#920

TBD

### Lab \#930

TBD

## Running the labs 

### Java

For information on running the Java lab, see chapter 15 in [Spark in Action, 2nd edition](http://jgp.ai/sia).

### PySpark

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer [Appendix K: Installing Spark in production and a few tips](https://livebook.manning.com/book/spark-in-action-second-edition/appendix-k/)). 

Step by step direction for lab \#100. You will need to adapt some steps for the other labs.

1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch15


2. Go to the lab in the Python directory

    cd net.jgp.books.spark.ch15/src/main/python/lab100_orders/


3. Execute the following spark-submit command to create a jar file to our this application

    spark-submit orderStatisticsApp.py

NOTE:- 
If we want to run PostgreSQL application, please do first first step as mentioned above 
and do the following two steps:

2. Go to the lab in the Python directory

    cd net.jgp.books.spark.ch15/src/main/python/lab910_nyc_to_postgresql/


3. Execute the following spark-submit command to create a jar file to our this application

    spark-submit --packages org.postgresql:postgresql:42.1.4 newYorkSchoolsToPostgreSqlApp.py


### Scala

Prerequisites:

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer [Appendix K: Installing Spark in production and a few tips](https://livebook.manning.com/book/spark-in-action-second-edition/appendix-k/)). 

Step by step direction for lab \#100. You will need to adapt some steps for the other labs.

1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch15

2. cd net.jgp.books.spark.ch15

3. Package application using sbt command

    sbt clean assembly

4. Run Spark/Scala application using spark-submit command as shown below:

    spark-submit --class net.jgp.books.spark.ch15.lab100_orders.OrderStatisticsScalaApp target/scala-2.12/SparkInAction2-Chapter15-assembly-1.0.0.jar  

NOTE:- 
If we want to run PostgreSQL application, please do first three steps as mentioned above 
and do the following two steps:

4. Run Spark/Scala application using spark-submit command as shown below:

    spark-submit --packages org.postgresql:postgresql:42.1.4 --class net.jgp.books.spark.ch15.lab910_nyc_to_postgresql.NewYorkSchoolsToPostgreSqlScalaApplication target/scala-2.12/SparkInAction2-Chapter15-assembly-1.0.0.jar  

Notes: 
 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 2. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://fb.com/SparkInAction/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).

[1]: https://data.cityofnewyork.us
