package edu.upc.essi.dtim.nextiajd.metafeatures.patterns.p

import edu.upc.essi.dtim.nextiajd.metafeatures.ProfileRGX

object AlphanumericPhrasesRGX extends ProfileRGX{

  lazy val pattern = "^[a-zA-Z0-9 ]*$".r // with space
  val dataType = "alphanumeric"
  val specificType = "phrases"


}
