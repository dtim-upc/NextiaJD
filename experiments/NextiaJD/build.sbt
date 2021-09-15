name := "NextiaJD_experiment1"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

libraryDependencies += "org.rogach" %% "scallop" % "3.5.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "3.0.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1" % "provided"


libraryDependencies += "org.antlr" % "antlr4-runtime" % "4.7.1"
libraryDependencies += "org.codehaus.janino" % "commons-compiler" % "3.0.16"
libraryDependencies += "org.codehaus.janino" % "janino" % "3.0.16"
libraryDependencies += "org.apache.parquet" % "parquet-column" % "1.10.1"
libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.10.1"
libraryDependencies += "com.univocity" % "univocity-parsers" % "2.9.0"

libraryDependencies += "edu.upc.essi.dtim.nextiajd" % "nextiajd_2.12" % "1.0.1"

assemblyJarName in assembly :=   "NextiaJD_experiment1-fatjar-0.1.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}