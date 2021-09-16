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

import edu.upc.essi.dtim.nextiajd.metafeatures.{MetaFeature, ProfilePattern, ProfileRGX}
import edu.upc.essi.dtim.nextiajd.metafeatures.patterns.p.{AlphabeticRGX, AlphanumericPhrasesRGX, AlphanumericRGX, DateRGX, DateTimeRGX, EmailRGX, IPRGX, NonAlphanumericRGX, NumericRGX, PhoneRGX, TimeRGX, URLRGX, UnknownRGX, UsernameRGX}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TrueDataType extends MetaFeature{

  val name: String = "basicType"
  val nameAtt: String = "basicType"
  val dataType: String = DataTypeProfilesEnum.all
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = false
  val normalizeType: Int = NormalizationTypeEnum.none
  val doc: String = "Classify column as numeric (0), alphabetic (1) ,alphanumeric (2), " +
    "date (3), time (4), binary(5)" +
    "phone, ips, url, emails"

  lazy val extractType = udf(extractTypeInfo(_: String): ProfilePattern)

  def getDataTypes( df: DataFrame, attName: String, cardinality: Double ): DataFrame = {

    if (cardinality <= 100 ) {
      df.filter(col(attName) =!= " ")
        .select(substring(col(attName), 0, 20).as(attName))
        .na.drop.distinct
        .withColumn(nameAtt, extractType(col(attName)))
        .select(s"${nameAtt}.*")
    } else {

      val randomS = df.select(substring(col(attName), 0, 15).as(attName))
        .na.drop.distinct.randomSplit(Array(0.1, 0.9))

      var tmpDataFrame = randomS(0).limit(100)

      if(tmpDataFrame.count() <= 10){
        tmpDataFrame = df.select(substring(col(attName), 0, 20).as(attName))
          .na.drop.distinct.limit(100)
      }

      tmpDataFrame
        .withColumn(nameAtt, extractType(col(attName)))
        .select(s"${nameAtt}.*")

    }


  }

  def extractTypeInfo(s: String): ProfilePattern = s match {
    case "" =>
      ProfilePattern(NonAlphanumericRGX.dataType, NonAlphanumericRGX.specificType)
    case " " =>
      ProfilePattern(AlphanumericRGX.dataType, AlphanumericRGX.specificType)
    case DateRGX.pattern(_*) =>
      ProfilePattern(DateRGX.dataType, DateRGX.specificType)
    case NumericRGX.pattern(_*) =>
      ProfilePattern(NumericRGX.dataType, NumericRGX.specificType)
    case EmailRGX.pattern(_*) =>
      ProfilePattern(EmailRGX.dataType, EmailRGX.specificType)
    case IPRGX.pattern(_*) =>
      ProfilePattern(IPRGX.dataType, IPRGX.specificType)
    case PhoneRGX.pattern(_*) =>
      ProfilePattern(PhoneRGX.dataType, PhoneRGX.specificType)
    case TimeRGX.pattern(_*) =>
      ProfilePattern(TimeRGX.dataType, TimeRGX.specificType)
    case DateTimeRGX.pattern(_*) =>
      ProfilePattern(DateTimeRGX.dataType, DateTimeRGX.specificType)
    case URLRGX.pattern(_*) =>
      ProfilePattern(URLRGX.dataType, URLRGX.specificType)
    case AlphabeticRGX.pattern(_*) =>
      ProfilePattern(AlphabeticRGX.dataType, AlphabeticRGX.specificType)
    case UsernameRGX.pattern(_*) =>
      ProfilePattern(UsernameRGX.dataType, UsernameRGX.specificType)
    case AlphanumericRGX.pattern(_*) =>
      ProfilePattern(AlphanumericRGX.dataType, AlphanumericRGX.specificType)
    case AlphanumericPhrasesRGX.pattern(_*) =>
      ProfilePattern(AlphanumericPhrasesRGX.dataType, AlphanumericPhrasesRGX.specificType)
    case NonAlphanumericRGX.pattern(_*) =>
      ProfilePattern(NonAlphanumericRGX.dataType, NonAlphanumericRGX.specificType)
    case _ =>
      ProfilePattern(UnknownRGX.dataType, UnknownRGX.specificType)
  }

}
