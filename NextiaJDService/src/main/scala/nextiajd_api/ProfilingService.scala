package nextiajd_api

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{SaveMode, SparkSession}

object ProfilingService {

  //https://stackoverflow.com/a/41990859
  def renamePartitionFile(spark: SparkSession, output: String): Unit = {
    import org.apache.hadoop.fs._
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path(output+"/part*"))(0).getPath().getName()

    fs.rename(new Path(output+"/" +file), new Path(output+"/"+output+".json"))
  }

  def profileFromCSV(spark: SparkSession, path: String, output: String): Unit = {
    import edu.upc.essi.dtim.NextiaJD.implicits
    val DF = spark.read.csv(path).cache()
    val profile = DF.attProfile()
    profile.attProfile().showAttProfile.repartition(1).write.mode(SaveMode.Overwrite).json(output)
    renamePartitionFile(spark,output)
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
    //DF_A.cache()
    val DF_B = spark.read.csv(pathB)
    //DF_B.cache()
    Discovery.preDist(DF_A,Seq(DF_B)).repartition(1).write.mode(SaveMode.Overwrite).json(output)
    renamePartitionFile(spark,output)
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
