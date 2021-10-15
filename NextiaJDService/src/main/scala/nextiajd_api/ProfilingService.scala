package nextiajd_api

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession

object ProfilingService {

  def profileFromCSV(spark: SparkSession, path: String, output: String): Unit = {
    import edu.upc.essi.dtim.NextiaJD.implicits
    val DF = spark.read.csv(path)
    val profile = DF.attProfile()
    DF.show()
    profile.attProfile().showAttProfile.show()
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

}
