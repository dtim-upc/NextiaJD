package edu.upc.essi.dtim.nextiajd.metafeatures.patterns.p

import edu.upc.essi.dtim.nextiajd.metafeatures.ProfileRGX

object NumericRGX extends ProfileRGX{

  lazy val pattern = "[-]?[0-9]+[,.]?[0-9]*([\\/][0-9]+[,.]?[0-9]*)*".r
  val dataType = "numeric"
  val specificType = "no-determined"

}
