package edu.upc.essi.dtim.nextiajd.metafeatures.patterns

import edu.upc.essi.dtim.nextiajd.metafeatures.MetaFeature
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}

object CardinalityNumeric extends MetaFeature {

  val name: String = "numberNumeric"
  val nameAtt: String = "PctNumeric"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.four
  val doc: String = "Number of numeric tuples"

}
