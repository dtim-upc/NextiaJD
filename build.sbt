name := "nextiajd"

version := "1.0"

scalaVersion := "2.12.13"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.12" % "3.1.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % "test"
libraryDependencies += "org.scalatest" %% "scalatest-wordspec" % "3.2.9" % "test"
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"