package edu.upc.essi.dtim.nextiajd.metafeatures.frequencies

import edu.upc.essi.dtim.nextiajd.metafeatures.{MetaFeature, Quantile}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}

object Octile1 extends MetaFeature with Quantile{

  val name: String = "frequency_1qo"
  val nameAtt: String = "frequency_1qo"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.octiles
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.zero
  val doc: String = "First octile of the frequency distribution count"

  val value: Double = 0.125
}
