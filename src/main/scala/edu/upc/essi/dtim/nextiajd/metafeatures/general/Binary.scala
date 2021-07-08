package edu.upc.essi.dtim.nextiajd.metafeatures.general

import edu.upc.essi.dtim.nextiajd.metafeatures.{DependantMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

object Binary extends MetaFeature with DependantMF{
  val name: String = "binary"
  val nameAtt: String = "isBinary"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.zero
  val doc: String = "Specifies 1 if the attribute is binary"

  def func(): Column = when(col( Cardinality.nameAtt) === 2, 1).otherwise(0)
}
