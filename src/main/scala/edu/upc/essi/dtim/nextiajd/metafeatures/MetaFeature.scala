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

package edu.upc.essi.dtim.nextiajd.metafeatures

import org.apache.spark.sql.Column

import scala.util.matching.Regex

trait MetaFeature {

  val name: String
  val nameAtt: String
  val dataType: String
  val aggregatedData: String
  val normalize: Boolean // true requires normalizing
  val normalizeType: Int
  val doc: String

}

trait BinaryMF {
  def func(columnName: String): Column
}

trait DefaultMF {
  def func(columnName: String): Column
}

trait DefaultWithConstantMF {

  def func(columnName:String, constant: Long): Column

}

trait DependantMF {

  def func(): Column
}

trait DependantWithConstantMF {

  def func(constant: Long): Column

}

trait Quantile {

  val value: Double

}

case class ProfilePattern ( dataType: String, specificType: String)
trait ProfileRGX {

  val pattern: Regex
  val dataType: String
  val specificType: String

}

trait CustomMF {


}
