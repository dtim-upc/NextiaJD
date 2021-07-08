package edu.upc.essi.dtim.nextiajd.metafeatures.binary

import edu.upc.essi.dtim.nextiajd.metafeatures.{BinaryMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column

class BestContainment extends MetaFeature with BinaryMF {

  val name: String = "bestContainment"
  val nameAtt: String = "bestContainment"
  val dataType: String = DataTypeProfilesEnum.all
  val doc: String = "bestContainment that can be achieved"
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.five

  def func(columnName: String): Column = ???
}
