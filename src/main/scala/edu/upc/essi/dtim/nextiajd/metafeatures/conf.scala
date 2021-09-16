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

import edu.upc.essi.dtim.nextiajd.metafeatures.binary._
import edu.upc.essi.dtim.nextiajd.metafeatures.frequencies._
import edu.upc.essi.dtim.nextiajd.metafeatures.general._
import edu.upc.essi.dtim.nextiajd.metafeatures.patterns.{CardEmail, CardGeneral, CardIP, CardOthers, CardPhone, CardPhrases, CardSpaces, CardURL, CardUsername, CardinalityAlphabetic, CardinalityAlphanumeric, CardinalityDateTime, CardinalityNonAlphanumeric, CardinalityNumeric}
import edu.upc.essi.dtim.nextiajd.metafeatures.words._
import edu.upc.essi.dtim.nextiajd.utils.AggregationDataEnum
import org.apache.spark.sql.{DataFrame}

object conf {


  val numberKWords = 20

  val octiles = Seq( Octile1,Octile2,Octile3,Octile4,Octile5, Octile6,Octile7 )

  val defaultMetafeatures = Seq( AvgWords, MaxWords, MinWords,  NumberWords,
    SDWords, Cardinality, AverageWord, AvgFrequency, FirstWord, LastWord,
    LongestWord, MaxFrequency, MinFrequency, SDFrequency, ShortestWord)

  val defaultConsMetafeatures = Seq( Incompleteness,  SDPercentage )


  val dependantMetafeatures = Seq(  Binary, Empty, Interquartile )
  val dependantConsMetafeatures = Seq(  MinPercentage,  Uniqueness,  Constancy,  MaxPercentage)


  val defaultMFRaw = defaultMetafeatures.filter(_.aggregatedData == AggregationDataEnum.rawValue)
  val defaultMFCount = defaultMetafeatures.filter(_.aggregatedData == AggregationDataEnum.countWords)
  val defaultMFFreq = defaultMetafeatures.filter(_.aggregatedData == AggregationDataEnum.frequency)

  val defaultConstRaw = defaultConsMetafeatures.filter(_.aggregatedData == AggregationDataEnum.rawValue)
  val defaultConstFreq = defaultConsMetafeatures.filter(_.aggregatedData == AggregationDataEnum.frequency)


  val customMetaFeatures = Seq(  Entropy,  FreqWord, FreqWordSoundex, TrueDataType ) //


  val AlphabeticStr = "alphabetic"
  val DateTimeStr = "datetime"
  val AlphanumericStr = "alphanumeric"
  val NumericStr = "numeric"
  val NonAlphanumericStr = "nonAlphanumeric"

  val typesList = List(AlphabeticStr -> CardinalityAlphabetic.nameAtt,
    DateTimeStr -> CardinalityDateTime.nameAtt, AlphanumericStr -> CardinalityAlphanumeric.nameAtt,
    NumericStr -> CardinalityNumeric.nameAtt, NonAlphanumericStr -> CardinalityNonAlphanumeric.nameAtt)


  val phoneStr = "phone"
  val emailStr = "email"
  val ipStr = "ip"
  val urlStr = "url"
  val usernameStr = "username"
  val generalStr = "general"
  val otherStr = "other"
  val spacesStr = "space"
  val phrasesStr = "phrases"

  val specifyTypesList = List(phoneStr -> CardPhone.nameAtt, emailStr -> CardEmail.nameAtt,
    ipStr -> CardIP.nameAtt, urlStr -> CardURL.nameAtt, usernameStr -> CardUsername.nameAtt,
    generalStr -> CardGeneral.nameAtt, otherStr -> CardOthers.nameAtt, spacesStr -> CardSpaces.nameAtt,
    phrasesStr -> CardPhrases.nameAtt)
//  val metaFeatures: Seq[MetaFeature] = defaultMetafeatures + defaultConsMetafeatures + dependantConsMetafeatures + dependantMetafeatures

    val all = defaultMetafeatures ++ octiles ++ defaultConsMetafeatures ++ dependantMetafeatures ++ dependantConsMetafeatures ++
              Seq( CardPhone, CardEmail, CardIP,CardURL, CardUsername, CardGeneral,CardOthers,  CardSpaces, CardPhrases) ++
              Seq(  Entropy,  FreqWord, FreqWordSoundex ) ++
              Seq(CardinalityAlphabetic,  CardinalityDateTime, CardinalityAlphanumeric, CardinalityNumeric, CardinalityNonAlphanumeric)



  //  def createProfileDF(df: DataFrame, metaFeatures: Seq[MetaFeature], nAtt: Int ): DataFrame = {
//
//    val names = metaFeatures.map(_.nameAtt)
//
//    val stackExprs = names.map(n => {
//      val cols = df.schema.filter(_.name.contains(s"_$n"))
//        .map(att => s"'${att.name.replace(s"_$n","")}',${att.name }" ).mkString(",")
//      s"stack(${nAtt},${cols}) as (attName,${n})"
//    }  )
//
//    stackExprs.map(x => df.selectExpr( x ) ).reduce(_.join(_, Seq("attName")))
//
//  }


}
