package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.stddev_pop

object SDFrequency extends MetaFeature with DefaultMF {

  val name: String = "val_size_std"
  val nameAtt: String = "frequency_sd"
  val dataType: String = DataTypeProfilesEnum.all
  val doc: String = "The standard deviation of the frequency distribution count"
  val aggregatedData: String = AggregationDataEnum.frequency
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.zero

  def func(columnName: String): Column = stddev_pop(columnName).as(s"${columnName}_${nameAtt}")

}
