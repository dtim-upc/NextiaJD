package nextiajd_api

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length < 1)
      throw new Exception("At least one argument required (--profile, --computeDistances, --predict)")

    val spark = SparkSession.builder.appName("NextiaJDService").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    if (args(0).contentEquals("--profile")) {
      if (args.length != 5)
        throw new Exception("Usage: [--profile --path path_to_csv --output path_to_profile.json] [--profiling --data " +
          "list of comma separated values --output path_to_profile.json]")
      if (args(1).contentEquals("--path")) {
        ProfilingService.profileFromCSV(spark, args(2), args(4))
      }
      else if (args(1).contentEquals("--data")) {
        ProfilingService.profileFromData(spark, args(2), args(4))
      }
    }
    else if (args(0).contentEquals("--computeDistances")) {
      if (args.length != 7)
        throw new Exception("Usage: [--computeDistances --pathA path_to_csv --pathB path_to_csv " +
          "--output path_vector.json] [--profiling --dataA list of comma separated values --dataB list of comma " +
          "separated values --output path_to_profile.json]")
      if (args(1).contentEquals("--pathA") && args(3).contentEquals("--pathB")) {
        ProfilingService.computeDistancesFromCSV(spark,args(2),args(4),args(6))
      }
      else if (args(1).contentEquals("--dataA") && args(3).contentEquals("--dataB")) {
        ProfilingService.computeDistancesFromData(spark,args(2),args(4),args(6))
      }


    }

  }
}
