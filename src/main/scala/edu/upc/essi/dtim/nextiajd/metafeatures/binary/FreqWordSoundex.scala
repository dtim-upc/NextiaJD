package edu.upc.essi.dtim.nextiajd.metafeatures.binary

import edu.upc.essi.dtim.nextiajd.metafeatures.MetaFeature
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}

object FreqWordSoundex extends MetaFeature {
  val name: String = "freqWordSoundex"
  val nameAtt: String = "freqWordSoundexContainment"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.two
  val doc: String = "The k most frequent string in a column using containment"
}
