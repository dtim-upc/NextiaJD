name := "NextiaJDService"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" //% "provided"
libraryDependencies += "edu.upc.essi.dtim.nextiajd" % "nextiajd_2.12" % "1.0.1"

mainClass := Some("nextiajd_api.Main")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}