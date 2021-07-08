package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultWithConstantMF, DependantWithConstantMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, stddev_pop}

object SDPercentage extends MetaFeature with DefaultWithConstantMF {
  val name: String = "val_pct_std"
  val nameAtt: String = "val_pct_std"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.frequency
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.four
  val doc: String = "pct"

  def func(columnName: String, constant: Long): Column = stddev_pop( col(columnName).divide(lit(constant)) ).as(s"${columnName}_${nameAtt}")
}
