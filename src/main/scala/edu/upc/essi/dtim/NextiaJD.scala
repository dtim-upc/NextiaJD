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

package edu.upc.essi.dtim

import edu.upc.essi.dtim.nextiajd.Discovery
import edu.upc.essi.dtim.nextiajd.Profiling.attributeProfile
import org.apache.spark.sql.DataFrame

object NextiaJD {


  implicit class implicits(df: DataFrame) {


    def attProfile(flag: Boolean): DataFrame = {
      cleanProfile(attributeProfile(df, flag))
    }

    def attProfile(): DataFrame = {
      cleanProfile(attributeProfile(df, false))
    }

    def rawProfile(flag: Boolean): DataFrame = {
      attributeProfile(df, flag)
    }

    def rawProfile(): DataFrame = {
      attributeProfile(df, false)
    }

    private[this] def cleanProfile(df: DataFrame) : DataFrame = {
      df.drop(Seq("freqWordCleanContainment", "binary", "isEmpty", "bestContainment"): _*)
        .withColumnRenamed("freqWordContainment", "FrequentWords")
        .withColumnRenamed("ds_name", "Dataset_name")
        .withColumnRenamed("att_name", "Attribute_name")
        .withColumnRenamed("freqWordSoundexContainment", "FrequentWordsInSoundex")
        .withColumnRenamed("wordsCntMax", "MaxNumberWords")
        .withColumnRenamed("wordsCntMin", "MinNumberWords")
        .withColumnRenamed("wordsCntAvg", "AvgNumberWords")
        .withColumnRenamed("wordsCntSd", "SdNumberWords")
    }



    def discovery(candidates: Seq[DataFrame]): DataFrame ={
      Discovery.discovery(df, candidates)
    }

    def discovery(candidates: Seq[DataFrame], attName: String): DataFrame ={
      Discovery.discovery(df, candidates, attName)
    }

    def discoveryPoor(candidates: Seq[DataFrame], attName: String): DataFrame ={
      Discovery.discovery(df, candidates, attName, showPoor = true)
    }

    def discoveryPoor(candidates: Seq[DataFrame]): DataFrame ={
      Discovery.discovery(df, candidates,  showPoor = true)
    }

    def discoveryModerate(candidates: Seq[DataFrame], attName: String): DataFrame ={
      Discovery.discovery(df, candidates, attName, showModerate = true)
    }

    def discoveryModerate(candidates: Seq[DataFrame]): DataFrame ={
      Discovery.discovery(df, candidates,  showModerate = true)
    }

    def discoveryAll(candidates: Seq[DataFrame], attName: String): DataFrame ={
      Discovery.discovery(df, candidates, attName, showAll = true)
    }

    def discoveryAll(candidates: Seq[DataFrame]): DataFrame ={
      Discovery.discovery(df, candidates,  showAll = true)
    }


  }

}
