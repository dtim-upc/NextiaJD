package edu.upc.essi.dtim

import edu.upc.essi.dtim.nextiajd.Discovery
import edu.upc.essi.dtim.nextiajd.Profiling.attributeProfile
import org.apache.spark.sql.DataFrame

object NextiaJD {


  implicit class implicits(df: DataFrame) {


    def attProfile(flag: Boolean): DataFrame = {
      attributeProfile(df, flag)
      df
    }

    def attProfile(): DataFrame = {
      attributeProfile(df, false)
      df
    }

    def showAttProfile: DataFrame = {
      attributeProfile(df, false)
        .drop(Seq("freqWordCleanContainment", "binary", "isEmpty", "bestContainment"): _*)
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
