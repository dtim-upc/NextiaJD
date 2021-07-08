package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{DependantMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object Interquartile extends MetaFeature with DependantMF{

  val name: String = "frequency_IQR"
  val nameAtt: String = "frequency_IQR"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.zero
  val doc: String = "Interquartile range of the frequency distribution count"

  def func(): Column = col( Octile6.nameAtt ).minus(col( Octile2.nameAtt))
}
