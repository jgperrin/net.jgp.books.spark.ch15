name := "SparkInAction2-Chapter15"

version := "1.0.0"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.0"

val postgreVersion = "42.1.4"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "org.postgresql"   %  "postgresql" % postgreVersion
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
