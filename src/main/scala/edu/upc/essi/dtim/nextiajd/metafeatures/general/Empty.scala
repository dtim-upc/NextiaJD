package edu.upc.essi.dtim.nextiajd.metafeatures.general

import edu.upc.essi.dtim.nextiajd.metafeatures.{DependantMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

object Empty extends MetaFeature with DependantMF{
  val name: String = "isempty"
  val nameAtt: String = "isempty"
  val dataType: String = DataTypeProfilesEnum.all
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = false
  val normalizeType: Int = NormalizationTypeEnum.zero
  val doc: String = "to identify which columns are empty"

  def func(): Column = when(col( Incompleteness.nameAtt) === 1, 1).otherwise(0)
}
