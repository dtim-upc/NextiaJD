package edu.upc.essi.dtim.nextiajd.metafeatures.general

import edu.upc.essi.dtim.nextiajd.metafeatures.{CustomMF, MetaFeature}
import edu.upc.essi.dtim.nextiajd.utils.{AggregationDataEnum, DataTypeProfilesEnum, NormalizationTypeEnum}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, log2, sum, when}

object Entropy extends MetaFeature with CustomMF{

  val name: String = "entropy"
  val nameAtt: String = "entropy"
  val dataType: String = DataTypeProfilesEnum.string
  val aggregatedData: String = AggregationDataEnum.rawValue
  val normalize: Boolean = true
  val normalizeType: Int = NormalizationTypeEnum.zero
  val doc: String = "The information entropy measures the distribution of an attribute"

  def value(freqPct: DataFrame, colName: String ): Any = {
    freqPct.select(
      (-sum(when(col(colName) === 0, lit(0)).otherwise(
        col(colName) * log2(col(colName))))).as(colName)).take(1).head.get(0)
  }

//  df = df.withColumn(Entropy.name, lit(value))

}
