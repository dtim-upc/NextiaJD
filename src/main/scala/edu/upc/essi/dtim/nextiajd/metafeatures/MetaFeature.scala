package edu.upc.essi.dtim.nextiajd.metafeatures

import org.apache.spark.sql.Column

import scala.util.matching.Regex

trait MetaFeature {

  val name: String
  val nameAtt: String
  val dataType: String
  val aggregatedData: String
  val normalize: Boolean // true requires normalizing
  val normalizeType: Int
  val doc: String

}

trait BinaryMF {
  def func(columnName: String): Column
}

trait DefaultMF {
  def func(columnName: String): Column
}

trait DefaultWithConstantMF {

  def func(columnName:String, constant: Long): Column

}

trait DependantMF {

  def func(): Column
}

trait DependantWithConstantMF {

  def func(constant: Long): Column

}

trait Quantile {

  val value: Double

}

case class ProfilePattern ( dataType: String, specificType: String)
trait ProfileRGX {

  val pattern: Regex
  val dataType: String
  val specificType: String

}

trait CustomMF {


}
