name := "spark-day-3"

version := "0.1"

scalaVersion := "2.11.8"

val sparkSql = "org.apache.spark" % "spark-sql_2.11" % "2.1.0"
val sparkCore = "org.apache.spark" %% "spark-core" % "2.1.0"
val postgres = "org.postgresql" % "postgresql" % "42.1.1"
val sparkStream ="org.apache.spark" %% "spark-streaming" % "2.1.0"

libraryDependencies ++= Seq(sparkCore, sparkSql, postgres, sparkStream)