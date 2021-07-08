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