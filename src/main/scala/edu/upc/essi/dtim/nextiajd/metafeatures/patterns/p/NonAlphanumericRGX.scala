package edu.upc.essi.dtim.nextiajd.metafeatures.patterns.p

import edu.upc.essi.dtim.nextiajd.metafeatures.ProfileRGX

object NonAlphanumericRGX extends ProfileRGX{

  lazy val pattern = "[^\\s\\p{L}\\p{N}]+".r

  val dataType = "nonAlphanumeric"
  val specificType = "no-determined"


}
