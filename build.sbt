name := "vijaypractice"

version := "1.0"

scalaVersion := "2.11.8"



libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0"

libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"

libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.47"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.13"
