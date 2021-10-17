package nextiajd_api

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession

object ProfilingService {

  def profileFromCSV(spark: SparkSession, path: String, output: String): Unit = {
    import edu.upc.essi.dtim.NextiaJD.implicits
    val DF = spark.read.csv(path)
    val profile = DF.attProfile()
    profile.attProfile().showAttProfile.repartition(1).write.json(output)
  }

  def profileFromData(spark: SparkSession, data: String, output: String): Unit = {
    //TODO Fix NextiaJD bug that does not allow to profile programatically-generated DataFrames
    /*
    import edu.upc.essi.dtim.NextiaJD.implicits
    import spark.implicits._

    val DF = data.split(",").toSeq.toDF()
    val profile = DF.attProfile()
    profile.show()
    */
    val tempFile = File.createTempFile("temp-", ".csv")
    new PrintWriter(tempFile) {
      try {
        write(data.replace(",", "\n"))
      } finally {
        close()
      }
    }
    profileFromCSV(spark, tempFile.getAbsolutePath, output)
  }

  def computeDistancesFromCSV(spark: SparkSession, pathA: String, pathB: String, output: String): Unit = {
    import edu.upc.essi.dtim.NextiaJD.implicits
    import edu.upc.essi.dtim.nextiajd.Discovery
    val DF_A = spark.read.csv(pathA)
    DF_A.show()
    val DF_B = spark.read.csv(pathB)
    Discovery.preDist(DF_A,Seq(DF_B)).show()
    Discovery.preDist(DF_A,Seq(DF_B)).repartition(1).write.json(output)
  }

  def computeDistancesFromData(spark: SparkSession, dataA: String, dataB: String, output: String): Unit = {
    val tempFileA = File.createTempFile("temp-", ".csv")
    new PrintWriter(tempFileA) {
      try {
        write(dataA.replace(",", "\n"))
      } finally {
        close()
      }
    }
    val tempFileB = File.createTempFile("temp-", ".csv")
    new PrintWriter(tempFileB) {
      try {
        write(dataB.replace(",", "\n"))
      } finally {
        close()
      }
    }
    computeDistancesFromCSV(spark, tempFileA.getAbsolutePath, tempFileB.getAbsolutePath, output)
  }

}
