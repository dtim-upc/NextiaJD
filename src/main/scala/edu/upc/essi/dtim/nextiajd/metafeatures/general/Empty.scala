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

import edu.upc.essi.dtim.nextiajd.metafeatures.{DependantMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

object Empty extends MetaFeature with DependantMF{
  val name: String = "isempty"
  val nameAtt: String = "isempty"
  val dataType: String = DataTypeProfilesEnum.all
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = false
  val normalizeType: Int = NormalizationTypeEnum.zero
  val doc: String = "to identify which columns are empty"

  def func(): Column = when(col( Incompleteness.nameAtt) === 1, 1).otherwise(0)
}
