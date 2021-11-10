name := "NextiaJDService"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" //% "provided"

resolvers += "SNAPSHOT" at "https://s01.oss.sonatype.org/content/repositories/snapshots/"
libraryDependencies += "edu.upc.essi.dtim.nextiajd" % "nextiajd_2.12" % "1.0.2.1-SNAPSHOT"

mainClass := Some("nextiajd_api.Main")


//https://stackoverflow.com/questions/54094482/sbt-assembly-deduplicate-error-with-org-apache-arrow
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) =>
    xs map {_.toLowerCase} match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
        MergeStrategy.discard
      case ps @ x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case x => MergeStrategy.first
}