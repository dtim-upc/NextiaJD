package edu.upc.essi.dtim.nextiajd.metafeatures.general

import edu.upc.essi.dtim.nextiajd.metafeatures.{DefaultMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.countDistinct

object Cardinality extends MetaFeature with DefaultMF {

  val name: String = "Cardinality"
  val nameAtt: String = "cardinality"
  val dataType: String = DataTypeProfilesEnum.all
  val doc: String = "Number of distinct values that appear within the column"
  val normalize: Boolean = true
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalizeType: Int = NormalizationTypeEnum.zero

  def func(columnName: String): Column = countDistinct(columnName).as(s"${columnName}_${nameAtt}")


}

//private var _dependant: Boolean = false
//private var _useFreqCnt: Boolean = false
//private var _useSoundexCnt: Boolean = false
//private var _isQuartile: Boolean = false
//private var _normalize: Boolean = true
//// (0) distance, (1) editDistance, (2) containment, (3) containment clean
//// (4) distance without z-score
//private var _normalizeType: Int = 0
//private var _useRawVal: Boolean = false
//private var _isCustom: Boolean = false
//private var _useWordCnt: Boolean = false