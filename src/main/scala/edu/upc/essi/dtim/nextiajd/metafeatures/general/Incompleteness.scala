package edu.upc.essi.dtim.nextiajd.metafeatures.general

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultWithConstantMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, isnull, lit, sum}
import org.apache.spark.sql.types.IntegerType

object Incompleteness extends MetaFeature with DefaultWithConstantMF {

  val name: String = "missing_values_pct"
  val nameAtt: String = "incompleteness"
  val dataType: String = DataTypeProfilesEnum.all
  val doc: String = "Number of missing values"
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.four

  def func(columnName: String, constant: Long): Column = sum( isnull( col(columnName)).cast(IntegerType))
    .divide( lit(constant) ).as(s"${columnName}_${nameAtt}")


}
