package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.min

object FirstWord extends MetaFeature with DefaultMF{
  val name: String = "firstWord"
  val nameAtt: String = "firstWord"
  val dataType: String = DataTypeProfilesEnum.all
  val aggregatedData: String = AggregationDataEnum.frequency
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.one
  val doc: String = "The first string entry in a column that is sorted alphabetically"

  def func(columnName: String): Column = min(s"${columnName}_raw").as(s"${columnName}_${nameAtt}")
}
