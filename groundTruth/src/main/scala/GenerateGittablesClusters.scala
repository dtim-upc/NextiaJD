import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.immutable.List

object GenerateGittablesClusters {

  val blackList = List(
    ".0041.%20Song%20Lyrics:%2520Beijing%2520Beijing%2520by%2520Wang%2520Feng.parquet.crc"
  )

  def readDataset(spark: SparkSession, path: String): Dataset[Row] = {
    import spark.implicits._
    try {
      val dataset = spark.read.parquet(path)
      dataset.withColumn("dataset", input_file_name())
        .withColumn("schema", typedLit(dataset.schema.filter(a => a.dataType.isInstanceOf[StringType]).map(a => a.name)))
        .select("dataset", "schema")
    } catch {
      case e => println("Error reading "+path)
        spark.read.parquet(path)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new Exception("Two arguments required (directory with parquet files, and output to write hdfs commands)")
    }

    val spark = SparkSession.builder.appName("GeneratGittablesClusters").master("local[*]").getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val all = os.list(os.Path(args(0)))
      .filter(p => blackList.contains(p.toString().split("/")(p.toString().split("/").length-1)))
      .map(p => readDataset(spark,p.toString()))
      .reduce(_.unionAll(_))

    all.foreach(_ => println())

    all.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv(args(1))
  }
}