name := "Scalability"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

libraryDependencies += "org.rogach" %% "scallop" % "3.5.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "3.0.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.1"  % "provided"
libraryDependencies += "commons-io" % "commons-io" % "2.6"

libraryDependencies += "edu.upc.essi.dtim.nextiajd" % "nextiajd_2.12" % "1.0.1"

assemblyJarName in assembly :=   "Scalability-fatjar-0.1.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

//"org.apache.hadoop" % "hadoop-common" % "2.7.0" exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy"),
//
