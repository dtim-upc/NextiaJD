package nextiajd_api

import java.io.{File, PrintWriter}
import java.nio.file.Files

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
    val DF = spark.read.csv(path)
    val profile = DF.attProfile(true)
    profile.attProfile(path,true).repartition(1).write.mode(SaveMode.Overwrite).json(output)
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
    val DF_A = spark.read.option("header",true).csv(pathA)
    val tempPathA = Files.createTempFile("a-",".parquet")
    DF_A.write.mode(SaveMode.Overwrite).parquet(tempPathA.toAbsolutePath.toString)
    val DF_A_parq = spark.read.parquet(tempPathA.toAbsolutePath.toString)
    val profileA = DF_A_parq.attProfile(pathA,true)
    //profileA.cache()
    val DF_B = spark.read.option("header",true).csv(pathB)
    val tempPathB = Files.createTempFile("b-",".parquet")
    DF_B.write.mode(SaveMode.Overwrite).parquet(tempPathB.toAbsolutePath.toString)
    val DF_B_parq = spark.read.parquet(tempPathB.toAbsolutePath.toString)
    val profileB = DF_B_parq.attProfile(pathB,true)
    //profileB.cache()
    profileA.distances(Seq(profileB)).repartition(1).write.mode(SaveMode.Overwrite).json(output)
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
