package edu.upc.essi.dtim.nextiajd.metafeatures.patterns.p

import edu.upc.essi.dtim.nextiajd.metafeatures.ProfileRGX

object URLRGX extends ProfileRGX {

  lazy val pattern = "((mailto\\:|www\\.|(news|(ht|f)tp(s?))\\:\\/\\/){1}\\S+)".r
  val dataType = "alphanumeric"
  val specificType = "url"


}
