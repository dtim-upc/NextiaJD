package edu.upc.essi.dtim.nextiajd.utils

import edu.upc.essi.dtim.nextiajd.metafeatures.MetaFeature
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}

object utils {

  def generatePathProfiling(path: String, filename: String, profileType: String): String = {
    val fn = filename.replace(".", "")
    path.replace(filename, s".${fn}/.${profileType}")
  }


  def generatePathProfiling(path:String, filename:String): ProfileFiles = {
    val fn = filename.replace(".", "")

    ProfileFiles(
      path.replace(filename, s".${fn}/.profilesStr"),
      path.replace(filename, s".${fn}/.profilesRaw"),
      path.replace(filename, s".${fn}/.profilesConstRaw"),
      path.replace(filename, s".${fn}/.profilesCount"),
      path.replace(filename, s".${fn}/.profilesConstFreq"),
      path.replace(filename, s".${fn}/.profilesFreq"),
      path.replace(filename, s".${fn}/.directProfiles"),
      path.replace(filename, s".${fn}/.octiles"),
    )

  }


  def toProfileDF(rawProfiles: DataFrame, metaFeatures: Seq[MetaFeature], nAtt: Int ): DataFrame = {

    val metaNames = metaFeatures.map(_.nameAtt)

    val stackExprs = metaNames.map( metaName => {
      // TODO: validate when the metafeature is not in the rawProfiles cause it produces "stack( nAtt, missing this param)"

      val cols = rawProfiles.schema.filter(_.name.contains(s"_$metaName"))
        .map(att => s"'${att.name.replace(s"_$metaName","")}',`${att.name }`" ).mkString(",")

      s"stack(${nAtt},${cols}) as (${ProfileDFEnum.attributeAtt},${metaName})"
    })


    val tmp = stackExprs.map(x => rawProfiles.selectExpr(x) )

    tmp.reduce(_.join(_,Seq(ProfileDFEnum.attributeAtt)))

  }


  def getFrequencyDF(df:DataFrame, attName: String, percentage: Boolean = false, nRows: Long = 1): DataFrame = {

    var tmp = df.filter(col(attName) =!= " ").select(col(attName).as(s"${attName}_raw"))
      .na.drop.groupBy(s"${attName}_raw")
      .agg(expr(s"count(*)").as(s"${attName}"))

    if(percentage) { // percentage
      tmp = tmp.select(col(attName).divide(nRows).as(attName))
    }
    tmp

  }

}
