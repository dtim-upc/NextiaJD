package edu.upc.essi.dtim.nextiajd.metafeatures.patterns.p

import edu.upc.essi.dtim.nextiajd.metafeatures.ProfileRGX

object PhoneRGX extends ProfileRGX{

  lazy val pattern = ("^(?:(?:\\(?(?:00|\\+)([1-4]\\d\\d|[1-9]\\d?)\\)?)?[\\-\\.\\ \\\\\\/]?)" +
    "?((?:\\(?\\d{1,}\\)?[\\-\\.\\ \\\\\\/]?){0,})(?:[\\-\\.\\ \\\\\\/]?(?:#|ext\\.?|extensio" +
    "n|x)[\\-\\.\\ \\\\\\/]?(\\d+))?$").r

  val dataType = "alphanumeric"
  val specificType = "phone"


}
