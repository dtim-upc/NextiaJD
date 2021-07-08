package edu.upc.essi.dtim.nextiajd.metafeatures.words

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import edu.upc.essi.dtim.nextiajd.metafeatures.SparkSessionTestWrapper
import org.apache.spark.sql.functions.{col, split, trim}
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should._

class AvgWordsTest extends AnyWordSpec with Matchers with DataFrameComparer with SparkSessionTestWrapper{

  import spark.implicits._

  "MetaFeature AvgWords" should {
    "calculate the average words in an attribute" in  {


      val sourceDF = Seq(
        ("javier", "welcome everybody", "mexico city"),
        ("luz", "hi! we are here to make an announcment ", "barcelona"),
        ("juan", "hi", "madrid")
      ).toDF("name", "speech", "city")

      val attributes = sourceDF.schema.map(_.name)
      val metaFeature = AvgWords


      val rawDF = sourceDF.select( attributes.map(att => trim(col(att)).as(att) ):_* )
      val countWordsDF = rawDF.select( attributes.map( att => functions.size(split(col(att), " ")).as(att) ):_* )
      val actualDF = countWordsDF.select(attributes.map(att => metaFeature.func(att) ):_* )


      val expectedSchema = List(
        StructField("name_wordsCntAvg", DoubleType),
        StructField("speech_wordsCntAvg", DoubleType),
        StructField("city_wordsCntAvg", DoubleType)
      )

      val expectedData = Seq(
        Row(1.0, 3.666,1.33)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)


    }
  }

}
