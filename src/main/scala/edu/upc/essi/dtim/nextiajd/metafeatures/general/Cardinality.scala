/*
 * Copyright 2020-2021 Javier de Jesus Flores Herrera, Sergi Nadal Francesch & Oscar Romero Moral
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 *
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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