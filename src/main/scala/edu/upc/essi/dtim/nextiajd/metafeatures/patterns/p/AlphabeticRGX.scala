package edu.upc.essi.dtim.nextiajd.metafeatures.patterns.p

import edu.upc.essi.dtim.nextiajd.metafeatures.ProfileRGX

object AlphabeticRGX extends ProfileRGX{

  lazy val pattern = "^[a-zA-Z]+$".r
  val dataType = "alphabetic"
  val specificType = "otherST"


}
