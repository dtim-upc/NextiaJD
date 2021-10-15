package nextiajd_api

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length < 1)
      throw new Exception("At least one argument required (--profiling, --predict)")

    val spark = SparkSession.builder.appName("NextiaJDService").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    if (args(0).contentEquals("--profiling")) {
      if (args.length != 5)
        throw new Exception("Usage: --profiling --path path_to_csv --output path_to_profile.json or --profiling --data list of comma separated values --output path_to_profile.json")
      if (args(1).contentEquals("--path")) {
        ProfilingService.profileFromCSV(spark, args(2), args(4))
      }
      else if (args(1).contentEquals("--data")) {
        ProfilingService.profileFromData(spark, args(2), args(4))
      }

    }

  }
}
