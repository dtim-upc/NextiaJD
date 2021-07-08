package edu.upc.essi.dtim.nextiajd.metafeatures.words

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.avg

object AvgWords extends MetaFeature with DefaultMF{
  val name: String = "AvgWords"
  val nameAtt: String = "wordsCntAvg"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.countWords
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.zero
  val doc: String = ""

  def func(columnName: String): Column = avg(columnName)
    .as(s"${columnName}_${nameAtt}")


}
