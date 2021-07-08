package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{length, min}

object ShortestWord extends MetaFeature with DefaultMF {
  val name: String = "shortWord"
  val nameAtt: String = "len_min_word"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.frequency
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.zero
  val doc: String = "The number of characters in the shortest value in the column"

  def func(columnName: String): Column = length(min(s"${columnName}_raw")).as(s"${columnName}_${nameAtt}")
}
