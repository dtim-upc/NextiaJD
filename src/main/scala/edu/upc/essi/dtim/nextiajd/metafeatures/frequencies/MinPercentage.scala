package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{DependantWithConstantMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object MinPercentage extends MetaFeature with DependantWithConstantMF {

  val name: String = "val_pct_min"
  val nameAtt: String = "val_pct_min"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.four
  val doc: String = "pct"

  def func(constant: Long): Column = col( MinFrequency.nameAtt ).divide( constant )
}
