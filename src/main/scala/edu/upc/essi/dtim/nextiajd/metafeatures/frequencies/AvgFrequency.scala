package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.avg

object AvgFrequency extends MetaFeature with DefaultMF{

  val name: String = "val_size_avg"
  val nameAtt: String = "frequency_avg"
  val dataType: String = DataTypeProfilesEnum.string // check if it should be all?
  val doc: String = "The average value of the frequency distribution count"
  val aggregatedData: String = AggregationDataEnum.frequency
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.zero

  def func(columnName: String): Column = avg(columnName).as(s"${columnName}_${nameAtt}")


}
