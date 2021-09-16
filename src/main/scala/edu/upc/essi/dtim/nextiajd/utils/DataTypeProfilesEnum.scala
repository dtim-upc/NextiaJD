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

package edu.upc.essi.dtim.nextiajd.utils

object DataTypeProfilesEnum {

  val all = "all"
  val string = "string"
  val numeric = "numeric"

}

object NormalizationTypeEnum {
  val zero = 0 // uses z-score
  val one = 1
  val two = 2
  val three = 3
  val four =4
  val five =5
  val none = 6//IDK

//  val normalizaDistance = 0 // uses z-score
//  val editDistance = 1
//  val containment = 2
//  val cleanContainment = 3
//  val cleanDistance =4
//  val type5 =5 //IDK
}

object AggregationDataEnum{

  val rawValue = "raw"
  val frequency = "Freq"
  val countWords = "countWords"
  val octiles = "octiles"

}


object ProfileDFEnum {

  val datasetAtt = "ds_name"
  val attributeAtt = "att_name"

}