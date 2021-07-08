package edu.upc.essi.dtim.nextiajd

import java.io.File

import edu.upc.essi.dtim.nextiajd.Profiling.attProfileDiscovery
import edu.upc.essi.dtim.nextiajd.metafeatures.conf.all
import edu.upc.essi.dtim.nextiajd.metafeatures.general.{Cardinality, Empty}
import edu.upc.essi.dtim.nextiajd.utils.{ProfileDFEnum, Unzip}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, StddevPop}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, functions}
import org.apache.spark.sql.functions.{array_intersect, col, desc, greatest, least, levenshtein, lit, lower, trim, udf, when}
import org.apache.spark.sql.types.{DoubleType, StringType}

import scala.reflect.io.Directory

object Discovery {


  def discovery(queryDataset: DataFrame, candidatesDatasets: Seq[DataFrame], queryAtt: String = "",
                showPoor: Boolean = false, showModerate: Boolean = false,
                showAll: Boolean = false): DataFrame = {

    val distances = preDist(queryDataset, candidatesDatasets, queryAtt)

    val filename = queryDataset.inputFiles(0).split("/").last
    val pathDiscoveryTmp = queryDataset.inputFiles(0).replace(filename, "discoveryTmp")


    distances.na.fill(0).write.mode(SaveMode.Overwrite).format("parquet").save(pathDiscoveryTmp)

    var distancescomputed = queryDataset.sparkSession.read.load(pathDiscoveryTmp)

    if(showModerate) {
      distancescomputed = distancescomputed.filter(col("flippedContainment") >= 0.083 )
    } else {
      // select pairs that fulfill cardinality proportion for qualities 3 and 4
      distancescomputed = distancescomputed.filter(col("flippedContainment") >= 0.125 )
    }

    val discovery = predict(distancescomputed)
    val directory = new Directory(new File(pathDiscoveryTmp))
    directory.deleteRecursively()

    if (showAll) {
      discovery.orderBy(desc("prediction"), desc("probability"))
        .select("query dataset", "query attribute", "candidate dataset", "candidate attribute",
          "quality", "probability")
    } else if ( showPoor && showModerate ) {
      discovery.filter( col("quality").isin("Poor", "Moderate") )
        .orderBy(desc("prediction"), desc("probability"))
        .select("query dataset", "query attribute", "candidate dataset", "candidate attribute",
          "quality", "probability")
    } else if ( showPoor ) {
      discovery.filter( col("quality").isin("Poor") )
        .orderBy(desc("prediction"), desc("probability"))
        .select("query dataset", "query attribute", "candidate dataset", "candidate attribute",
          "quality", "probability")
    } else if (showModerate) {
      discovery.filter( col("quality").isin("Moderate") )
        .orderBy(desc("prediction"), desc("probability"))
        .select("query dataset", "query attribute", "candidate dataset", "candidate attribute",
          "quality", "probability")
    } else {
      discovery.filter( col("quality").isin( "High", "Good" ) )
        .orderBy(desc("prediction"), desc("probability"))
        .select("query dataset", "query attribute", "candidate dataset", "candidate attribute",
          "quality", "probability")
    }

  }



  def preDist(queryDataset: DataFrame, candidatesDatasets: Seq[DataFrame],
              queryAtt: String = "", filterEnable: String = "true"): DataFrame = {
    val profiles = getProfiles(queryDataset, candidatesDatasets, queryAtt, filterEnable)

    val filename = queryDataset.inputFiles(0).split("/").last
    val pathDiscoveryTmp1 = queryDataset.inputFiles(0).replace(filename, ".discovery/.normalization")

    normalizeProfiles(profiles, "nominal").write.mode(SaveMode.Overwrite).format("parquet").save(pathDiscoveryTmp1)
    val normalizedP = queryDataset.sparkSession.read.load(pathDiscoveryTmp1)

    val fileName = queryDataset.inputFiles(0).split("/").last


    val pathDiscoveryTmp2 = queryDataset.inputFiles(0).replace(filename, ".discovery/.pairs")
    createPairs(fileName, normalizedP).write.mode(SaveMode.Overwrite).format("parquet").save(pathDiscoveryTmp2)
    val pairs = queryDataset.sparkSession.read.load(pathDiscoveryTmp2)

    distances(pairs)
  }

  def getProfiles(queryDataset: DataFrame, candidatesDatasets: Seq[DataFrame],
                  queryAtt: String = "", filterEnable: String = "true"): DataFrame = {

    var profiles = attProfileDiscovery(queryDataset)
    if (queryAtt != "") {
      profiles = attProfileDiscovery(queryDataset).filter(col(ProfileDFEnum.attributeAtt) === queryAtt)
    }

    for (i <- 0 to candidatesDatasets.size-1) {
      var profilesTmp = attProfileDiscovery(candidatesDatasets(i))
      if (!profilesTmp.head(1).isEmpty) {
        profiles = profiles.union(profilesTmp)
      }
    }

    if(filterEnable == "true") {

      profiles = profiles.filter(col(Empty.nameAtt) === 0)
        .filter(col("dataType") =!= "numeric")
        .filter(col("specificType") =!= "phone")
        .filter(col("specificType") =!= "datetime")
        .filter(col("specificType") =!= "time")
        .filter(col("specificType") =!= "date")
    } else {
      profiles = profiles.filter(col(Empty.nameAtt) === 0)
    }
    profiles = profiles.withColumn("cardinalityRaw", col(Cardinality.nameAtt).cast(DoubleType))
    profiles = profiles.withColumn("bestContainment", col("bestContainment").cast(DoubleType))
    profiles
  }


  def normalizeProfiles(df: DataFrame, metaType: String): DataFrame = {

    val cols = all.filter(_.normalize).filter(_.normalizeType == 0)

    val aggExprsAvg = Seq((child: Expression) => Average(child).toAggregateExpression())
      .flatMap { func => cols.map(c => new Column(Cast(func(new Column(c.nameAtt).expr), StringType))
        .as(s"${c.nameAtt}_avg"))
      }
    val aggExprsSD = Seq((child: Expression) => StddevPop(child).toAggregateExpression())
      .flatMap { func => cols.map(c => new Column(Cast(func(new Column(c.nameAtt).expr), StringType))
        .as(s"${c.nameAtt}_sd"))
      }
    val aggExprs = aggExprsAvg ++ aggExprsSD
    val avgSD = df.select(aggExprs: _*).take(1).head

    var zScoreDF = df
    val colnames = cols.map(_.nameAtt)
    for (c <- colnames) {
      zScoreDF = zScoreDF.withColumn(c, when(lit(avgSD.getAs(s"${c}_sd")) === 0,
        (col(c) - lit(avgSD.getAs(s"${c}_avg"))) /  lit(1))
        .otherwise((col(c) - lit(avgSD.getAs(s"${c}_avg"))) / lit(avgSD.getAs(s"${c}_sd")))
      )
    }
    zScoreDF.toDF()
  }

  def createPairs(fileName: String, normalizeProfiles: DataFrame): DataFrame = {

    val metafeatures = normalizeProfiles.schema.map(_.name)

    val candidateAtt = normalizeProfiles.filter(col(ProfileDFEnum.datasetAtt) =!= fileName)
      .select(metafeatures.map(x => col(x).as(s"${x}_2")): _*)

    val queryAtt = normalizeProfiles.filter(col(ProfileDFEnum.datasetAtt) === fileName)

    if (queryAtt.count() == 1) {

      queryAtt.withColumn("key", lit("1"))
        .join(candidateAtt.withColumn("key", lit("1")), "key")
        .drop("key")


    } else {

      queryAtt.crossJoin(candidateAtt)
    }
  }


  def distances(pairsAtt: DataFrame): DataFrame = {
    var pairs = pairsAtt

    //    bestContainment raw is cardinality
    pairs = pairs.withColumn("flippedContainment",
      least(col("bestContainment"), col("bestContainment_2"))
        /greatest(col("bestContainment"), col("bestContainment_2"))
    )

    pairs = pairs.withColumn("worstBestContainment", col("flippedContainment"))

    val metaFeatures = all

    for (metafeature <- metaFeatures.filter(_.normalize)) {
      metafeature.normalizeType match {
        case 3 => // probably delete it
          pairs = pairs.withColumn(metafeature.nameAtt,
            functions.size(array_intersect(col(metafeature.nameAtt), col(s"${metafeature.nameAtt}_2")))
              /greatest(functions.size(col(metafeature.nameAtt)), functions.size(col(s"${metafeature.nameAtt}_2")))
          )
        case 2 =>
          pairs = pairs.withColumn(metafeature.nameAtt,
            functions.size(array_intersect(col(metafeature.nameAtt), col(s"${metafeature.nameAtt}_2")))
              /greatest(functions.size(col(metafeature.nameAtt)), functions.size(col(s"${metafeature.nameAtt}_2")))
          )
        case 1 => // edit distance
          pairs = pairs.withColumn(metafeature.nameAtt,
            levenshtein(
              lower(trim(col(metafeature.nameAtt))),
              lower(trim(col(s"${metafeature.nameAtt}_2")))
            )/ greatest(functions.length(col(metafeature.nameAtt)), functions.length(col(s"${metafeature.nameAtt}_2")))
          )

        case 5 =>

          pairs = pairs.withColumn("bestContainment",
            least(col("bestContainment"), col("bestContainment_2"))/col("bestContainment")
          )

        case _ => // 0 y 4
          pairs = pairs.withColumn(
            metafeature.nameAtt, col(metafeature.nameAtt) - col(s"${metafeature.nameAtt}_2"))

      }
    }

    pairs = pairs.withColumn(
      "name_dist", levenshtein(lower(trim(col(ProfileDFEnum.attributeAtt))), lower(trim(col(s"${ProfileDFEnum.attributeAtt}_2"))))
        / greatest(functions.length(col(ProfileDFEnum.attributeAtt)), functions.length(col(s"${ProfileDFEnum.attributeAtt}_2")))
    )
    pairs = pairs.drop(metaFeatures.filter(_.normalize).map(x => s"${x.nameAtt}_2"): _*)
    pairs
  }


  def predict(matchingNom: DataFrame): DataFrame = {

    val pathM = Unzip.unZipIt(getClass.getResourceAsStream("/models.zip") )

    val cvModel0 = CrossValidatorModel.load(s"${pathM}/models/class0")
    val cvModel1 = CrossValidatorModel.load(s"${pathM}/models/class1")
    val cvModel2 = CrossValidatorModel.load(s"${pathM}/models/class2")
    val cvModel3 = CrossValidatorModel.load(s"${pathM}/models/class3")
    val cvModel4 = CrossValidatorModel.load(s"${pathM}/models/class4")

    val dropCols = Seq("features", "rawPrediction", "probability", "prediction")

    val p0 = cvModel0.transform(matchingNom)

    val resultsM0 = p0.withColumn("p0", second(col("probability")))
      .drop(dropCols: _*)

    val p1 = cvModel1.transform(resultsM0)

    val resultsM1 = p1.withColumn("p1", second(col("probability"))).drop(dropCols: _*)

    val p2 = cvModel2.transform(resultsM1)

    val resultsM2 = p2.withColumn("p2", second(col("probability"))).drop(dropCols: _*)


    val p3 = cvModel3.transform(resultsM2)

    val resultsM3 = p3.withColumn("p3", second(col("probability"))).drop(dropCols: _*)

    val predAll = cvModel4.transform(resultsM3).withColumn("p4", second(col("probability")))
      .drop(dropCols: _*).select(ProfileDFEnum.datasetAtt, ProfileDFEnum.attributeAtt, s"${ProfileDFEnum.datasetAtt}_2", s"${ProfileDFEnum.attributeAtt}_2", "p0", "p1",
      "p2", "p3", "p4", "flippedContainment", "bestContainment", "cardinalityRaw", "cardinalityRaw_2")


    val directory = new Directory(new File(pathM))
    directory.deleteRecursively()



    predAll.withColumn("prediction", rank(
      col("p0"), col("p1"), col("p2"), col("p3"), col("p4"),
      col("cardinalityRaw"), col("cardinalityRaw_2")
    ) )
      .withColumn("probability",
        when(col("prediction") === 4, col("p4"))
          .when(col("prediction") === 3, col("p3"))
          .when(col("prediction") === 2, col("p2"))
          .when(col("prediction") === 1, col("p1"))
          .otherwise(col("p0"))
      )
      .withColumn("quality",
        when(col("prediction") === 4, lit("High"))
          .when(col("prediction") === 3, lit("Good"))
          .when(col("prediction") === 2, lit("Moderate"))
          .when(col("prediction") === 1, lit("Poor"))
          .otherwise(lit("None"))
      )

      .select(col(ProfileDFEnum.datasetAtt).as("query dataset"),
        col(ProfileDFEnum.attributeAtt).as("query attribute"),
        col(s"${ProfileDFEnum.datasetAtt}_2").as("candidate dataset"),
        col(s"${ProfileDFEnum.attributeAtt}_2").as("candidate attribute"), col("prediction"),
        col("quality"), col("probability"))



  }


  def assignClass(num0: Double, num1: Double, num2: Double, num3: Double,
                  num4: Double, cardinalityQ: Double, cardinalityC: Double): Int = {
    var s = Seq((num4, 4), (num3, 3), (num2, 2), (num1, 1), (num0, 0))
    //    var s = Seq(num0, num1, num2, num3, num4).zipWithIndex
    var index: Int = 0
    var i = 0
    while (i <= 4) {
      index = s.maxBy(_._1)._2


      if (validateLabel(index, cardinalityQ, cardinalityC) ) {
        i = 5
      } else {
        s = s.filter(_._2 != index)
      }
      i = i + 1
    }

    if (index != 0) {
      val diff0 = (s((index -4).abs)._1 -num0).abs
      if (diff0 <= 0.1  || num0 >= 0.5 ) {
        index = s.filter(_._2 < index).maxBy(_._1)._2
      }
    }
    index


  }

  def validateLabel(index: Double, cardinalityQ: Double,
                    cardinalityC: Double): Boolean = index match {
    case 2 =>
      if (cardinalityQ/cardinalityC >= 0.083 ) true else false
    case 3 =>
      if (cardinalityQ/cardinalityC >= 0.125) true else false
    case 4 =>
      if (cardinalityQ/cardinalityC >= 0.25) true else false
    case _ =>
      true
  }


  val rank = udf(assignClass(_: Double, _: Double, _: Double
    , _: Double, _: Double, _: Double, _: Double): Int)

  val second = udf((v: org.apache.spark.ml.linalg.Vector) => v.toArray(1))




}

