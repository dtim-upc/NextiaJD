package edu.upc.essi.dtim.nextiajd.metafeatures.patterns.p

import edu.upc.essi.dtim.nextiajd.metafeatures.ProfileRGX

object DateTimeRGX extends ProfileRGX{

  lazy val pattern = ("^((((([13578])|(1[0-2]))[\\-\\/\\s]?(([1-9])|([1-2][0-9])|(3[01])))|" +
    "((([469])|(11))[\\-\\/\\s]?(([1-9])|([1-2][0-9])|(30)))|(2[\\-\\/\\s]?(([1-9])|([1-2][0-9]" +
    "))))[\\-\\/\\s]?\\d{4})(\\s((([1-9])|(1[02]))\\:([0-5][0-9])((\\s)|(\\:([0-5][0-9])\\s))([" +
    "AM|PM|am|pm]{2,2})))?$").r
  val dataType = "datetime"
  val specificType = "datetime"



}
