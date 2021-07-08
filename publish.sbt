//https://www.scala-sbt.org/release/docs/Using-Sonatype.html


ThisBuild / organization := "edu.upc.essi.dtim.nextiajd"
ThisBuild / organizationName := "DTIM"
ThisBuild / organizationHomepage := Some(url("https://www.essi.upc.edu/dtim/"))
ThisBuild / scalaVersion     := "2.12"

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/dtim-upc/NextiaJD"),
    "scm:git@github.com:dtim-upc/NextiaJD.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "Javier",
    name  = "Javier Flores",
    email = "jflores@essi.upc.edu",
    url   = url("https://www.essi.upc.edu/dtim/people/jflores")
  )
)


ThisBuild / description := "NextiaJD is a library that supports data discovery based on data profiles and machine learning algorithms to find joinable attributes in heterogeneous datasets"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://www.essi.upc.edu/dtim/nextiajd/"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true