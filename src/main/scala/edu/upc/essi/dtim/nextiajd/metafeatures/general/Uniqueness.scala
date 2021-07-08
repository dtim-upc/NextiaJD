package edu.upc.essi.dtim.nextiajd.metafeatures.general

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultWithConstantMF, DependantMF, DependantWithConstantMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

object Uniqueness extends MetaFeature with DependantWithConstantMF {

  val name: String = "distinct_values_pct"
  val nameAtt: String = "uniqueness"
  val dataType: String = DataTypeProfilesEnum.all
  val doc: String = "Number of distinct values divided by the number of rows"
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.four

  def func(constant: Long): Column = col( Cardinality.nameAtt ).divide(lit(constant))

}
