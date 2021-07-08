package edu.upc.essi.dtim.nextiajd.metafeatures.patterns.p

import edu.upc.essi.dtim.nextiajd.metafeatures.ProfileRGX

object UsernameRGX extends ProfileRGX{

  lazy val pattern = "^[a-z0-9_-]{3,16}$".r // having a length of 3 to 16 characters
  val dataType = "alphanumeric"
  val specificType = "username"


}
