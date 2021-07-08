package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{avg, col, length}

object AverageWord extends MetaFeature with DefaultMF {
  val name: String = "avgWord"
  val nameAtt: String = "len_avg_word"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.zero
  val doc: String = "Average length of words in terms of characters"

  def func(columnName: String): Column = avg(length(col(columnName))).as(s"${columnName}_${nameAtt}")
}
