package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{DependantWithConstantMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object Constancy extends MetaFeature with DependantWithConstantMF {

  val name: String = "constancy"
  val nameAtt: String = "constancy"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.rawValue //technically is freq count
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.four
  val doc: String = "Frequency of most frequent value divided by number of rows"

  def func(constant: Long): Column = col(  MaxFrequency.nameAtt ).divide( constant)

}
