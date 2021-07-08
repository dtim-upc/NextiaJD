package edu.upc.essi.dtim.nextiajd.metafeatures.patterns.p

import edu.upc.essi.dtim.nextiajd.metafeatures.ProfileRGX

object UnknownRGX extends ProfileRGX{
  lazy val pattern = "".r
  val dataType = "unknown"
  val specificType = "unknown-specific-type"

}
