package edu.upc.essi.dtim.nextiajd.metafeatures.patterns

import edu.upc.essi.dtim.nextiajd.metafeatures.MetaFeature
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}

object CardinalityNonAlphanumeric extends MetaFeature {

  val name: String = "numberNonAlphanumeric"
  val nameAtt: String = "PctNonAlphanumeric"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.four
  val doc: String = "Number of non-alphanumeric tuples"

}
