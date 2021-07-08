package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.min

object MinFrequency extends MetaFeature with DefaultMF{

  val name: String = "val_size_min"
  val nameAtt: String = "frequency_min"
  val dataType: String = DataTypeProfilesEnum.string
  val doc: String = "The minimum value of the frequency distribution count"
  val aggregatedData: String = AggregationDataEnum.frequency
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.zero

  def func(columnName: String): Column = min(columnName).as(s"${columnName}_${nameAtt}")


}
