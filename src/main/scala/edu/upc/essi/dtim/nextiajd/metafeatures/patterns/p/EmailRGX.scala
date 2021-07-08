package edu.upc.essi.dtim.nextiajd.metafeatures.patterns.p

import edu.upc.essi.dtim.nextiajd.metafeatures.ProfileRGX

object EmailRGX extends ProfileRGX{

  lazy val pattern = "^([a-z0-9_\\.\\+-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})$".r
  val dataType = "alphanumeric"
  val specificType = "email"

}
