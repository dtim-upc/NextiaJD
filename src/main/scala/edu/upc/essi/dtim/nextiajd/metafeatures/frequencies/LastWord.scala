package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.max

object LastWord extends MetaFeature with DefaultMF{
  val name: String = "lastWord"
  val nameAtt: String = "lastWord"
  val dataType: String = DataTypeProfilesEnum.all
  val aggregatedData: String = AggregationDataEnum.frequency
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.one
  val doc: String = "The last string entry in a column that is sorted alphabetically"

  def func(columnName: String): Column = max(s"${columnName}_raw").as(s"${columnName}_${nameAtt}")
}
