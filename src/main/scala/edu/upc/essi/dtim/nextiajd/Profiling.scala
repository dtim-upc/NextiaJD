/*
 * Copyright 2020-2021 Javier de Jesus Flores Herrera, Sergi Nadal Francesch & Oscar Romero Moral
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 *
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.upc.essi.dtim.nextiajd

import edu.upc.essi.dtim.nextiajd.Discovery.discovery
import edu.upc.essi.dtim.nextiajd.metafeatures.binary.{FreqWord, FreqWordSoundex}
import edu.upc.essi.dtim.nextiajd.metafeatures.MetaFeature
import edu.upc.essi.dtim.nextiajd.metafeatures.conf.{customMetaFeatures, defaultConstFreq, defaultConstRaw, defaultMFCount, defaultMFFreq, defaultMFRaw, defaultMetafeatures, dependantConsMetafeatures, dependantMetafeatures, numberKWords, octiles, specifyTypesList, typesList}
import edu.upc.essi.dtim.nextiajd.metafeatures.general.{Cardinality, Entropy, TrueDataType}
import edu.upc.essi.dtim.nextiajd.utils.{ProfileDFEnum, ProfileFiles}
import edu.upc.essi.dtim.nextiajd.utils.utils.{generatePathProfiling, getFrequencyDF, toProfileDF}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}
import org.apache.spark.sql.functions.{col, desc, expr, lit, lower, size, soundex, split, trim, typedLit}


object Profiling {


  def attProfileDiscovery(df: DataFrame): DataFrame = {
    val filename = df.inputFiles(0).split("/").last
    attributeProfile(df).withColumn("bestContainment", col(Cardinality.nameAtt))
      .withColumn(ProfileDFEnum.datasetAtt, lit(filename))
  }


//
  def attributeProfile(df: DataFrame, recompute: Boolean = false): DataFrame = {


    val path = df.inputFiles(0)
    // TODO: Handle when there is no file or multi name files are used
    val filename = path.split("/").last
    val profileFiles = generatePathProfiling(path,filename)

    if( recompute ) {
      computeStringProfile(df, profileFiles)
    } else {

      try {
        return df.sparkSession.read.load(profileFiles.profilesStr)
      } catch {
        case e:
          AnalysisException =>
          // when is produced?
          computeStringProfile(df, profileFiles)
        case _:
          // TODO: handle other exceptions
          Throwable => println("Got some other kind of Throwable exception")
          //return empty profile
          df.sparkSession.emptyDataFrame

      }

    }

  }


  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }
  def computeStringProfile(df: DataFrame, profileFiles: ProfileFiles): DataFrame = {

    val nRows = df.count()

    val strAttributes = df.schema.fields.filter(att => att.dataType.isInstanceOf[StringType])
    val rawDF = df.select( strAttributes.map(att => trim(col(att.name)).as(att.name) ):_* )

    val directMF = computeDirectMetaFeatures(rawDF, strAttributes, nRows, profileFiles)

    val directAndOctiles = computeOctiles(directMF, strAttributes, rawDF, nRows, profileFiles)

    val directAndOctilesAndDependant = computeDependantStrProfiles(directAndOctiles, nRows)

    val profiles = strAttributes.map( att => computeCustom(rawDF, att.name, customMetaFeatures, directAndOctilesAndDependant) )
                    .reduce(_.union(_))
    profiles.write.mode(SaveMode.Overwrite).parquet(profileFiles.profilesStr)
    profiles
  }


  def computeDirectMetaFeatures(rawDF: DataFrame, strAttributes: Seq[StructField],  nrows:Long, profileFiles: ProfileFiles): DataFrame = {
    // prepare data frames for executing features
    val countWordsDF = rawDF.select( strAttributes.map( att => size(split(col(att.name), " ")).as(att.name) ):_* )
    val frequencyDFs = strAttributes.map(att => att.name ->  getFrequencyDF(rawDF, att.name)  )

    // computes cardinality
    val defaultProfilesRaw = rawDF.select( defaultMFRaw
      .flatMap( mf => strAttributes.map( x => mf.func(x.name)  )):_* )

    // creates dataframe
  toProfileDF( defaultProfilesRaw, defaultMFRaw, strAttributes.size).write.mode(SaveMode.Overwrite).parquet(profileFiles.profilesRaw)
    val profilesRaw =   rawDF.sparkSession.read.parquet(profileFiles.profilesRaw)

  // compute raw with constant variable
    val defaultConstProfilesRaw = rawDF.select( defaultConstRaw
      .flatMap( mf => strAttributes.map( x => mf.func(x.name, nrows)  )):_* )
  toProfileDF( defaultConstProfilesRaw, defaultConstRaw, strAttributes.size ).write.mode(SaveMode.Overwrite).parquet(profileFiles.profilesConstRaw)
    val profilesConstRaw = rawDF.sparkSession.read.parquet(profileFiles.profilesConstRaw)

    //computes AvgWords-wordsCntAvg MaxWords-wordsCntMax MinWords-wordsCntMin words-numberWords SdWords-wordsCntSd
    val defaultProfilesCountW = countWordsDF.select(defaultMFCount
      .flatMap( mf => strAttributes.map( x => mf.func(x.name)  )):_* )
  toProfileDF( defaultProfilesCountW, defaultMFCount, strAttributes.size ).write.mode(SaveMode.Overwrite).parquet(profileFiles.profilesCount)
    val profilesCount = rawDF.sparkSession.read.parquet(profileFiles.profilesCount)


  val defaultConstProfilesFreq = frequencyDFs.map{ case (att , df) => df.select( defaultConstFreq.map( mf => mf.func(att, nrows) ):_* )  }
  var profilesConstFreq = defaultConstProfilesFreq.map(df => toProfileDF( df, defaultConstFreq, 1 )).reduce(_.union(_))
  profilesConstFreq.write.mode(SaveMode.Overwrite).parquet(profileFiles.profilesConstFreq)
  profilesConstFreq = rawDF.sparkSession.read.parquet(profileFiles.profilesConstFreq)


    // computes avgWord-len_avg_word val_size_avg-frequency_avg firstWord-firstWord lastWord-lastWord longWord-len_max_word
    // val_size_max-frequency_max val_size_min-frequency_min val_size_std-frequency_sd shortWord-len_min_word
    val defaultProfilesFreq = frequencyDFs.map{ case (att , df) =>
      df.select( defaultMFFreq.map( mf => mf.func(att)  ):_* )  }

    var profilesFreq = defaultProfilesFreq.map(df => toProfileDF( df, defaultMFFreq, 1 )).reduce(_.union(_))
  profilesFreq.write.mode(SaveMode.Overwrite).parquet(profileFiles.profilesFreq)
    profilesFreq = rawDF.sparkSession.read.parquet(profileFiles.profilesFreq)


    val profiledf = Seq(profilesFreq, profilesRaw, profilesCount, profilesConstRaw, profilesConstFreq )
      .reduce(_.join(_, ProfileDFEnum.attributeAtt))

    profiledf.write.mode(SaveMode.Overwrite).parquet(profileFiles.directProfiles)
    rawDF.sparkSession.read.parquet(profileFiles.directProfiles)

  }


  def computeOctiles(df: DataFrame, strAttributes: Seq[StructField], rawDF: DataFrame, nRows: Long, profileFiles: ProfileFiles): DataFrame = {
//    var profiledf = df
    // octiles
    val o = octiles.zipWithIndex

    val dfs = strAttributes.map(att => {
      val q = getFrequencyDF(rawDF, att.name, true, nRows).stat
        .approxQuantile(att.name, octiles.map(_.value).toArray, 0.0)


      var octilesDF = df.filter(col(ProfileDFEnum.attributeAtt) === att.name)
      if(q.isEmpty) {
        // attribute has 100% of missing values
        octiles.map(m => octilesDF = octilesDF.withColumn(m.nameAtt, lit(0)))
      } else {
        q.zipWithIndex.map(tuple => octilesDF = octilesDF.withColumn(octiles(tuple._2).nameAtt, lit(tuple._1)) )
      }
      octilesDF
    })
    dfs.reduce(_.union(_)).write.mode(SaveMode.Overwrite).parquet(profileFiles.octiles)
    df.sparkSession.read.parquet(profileFiles.octiles)

  }

  def computeDependantStrProfiles( profile: DataFrame, nRows: Long): DataFrame = {

    val dependant = dependantMetafeatures.map(mf => profile.withColumn(mf.nameAtt, mf.func())
      .select( ProfileDFEnum.attributeAtt,mf.nameAtt) )
      .reduce(_.join(_, ProfileDFEnum.attributeAtt))

    val dependantConst = dependantConsMetafeatures.map(mf => profile.withColumn(mf.nameAtt, mf.func(nRows))
      .select( ProfileDFEnum.attributeAtt,mf.nameAtt) )
      .reduce(_.join(_, ProfileDFEnum.attributeAtt))

    profile.join(dependant, ProfileDFEnum.attributeAtt)
      .join(dependantConst, ProfileDFEnum.attributeAtt)
  }






  def computeCustom( raw: DataFrame, colName: String,
                    mf: Seq[MetaFeature], profiles: DataFrame)
  : DataFrame = {

    var df: DataFrame = profiles.filter(col(ProfileDFEnum.attributeAtt) === colName)

    mf.foreach(meta => meta.name match {

      case Entropy.name =>

        val e = Entropy.value( getFrequencyDF(raw, colName, percentage = true)  , colName)
        df = df.withColumn(Entropy.name, lit(e))


      case TrueDataType.name =>
        val cardinalityName =  Cardinality.nameAtt
        val cardinality = profiles.filter(col(ProfileDFEnum.attributeAtt) === colName)
              .select(col(cardinalityName).cast(DoubleType)).collect()(0).getAs[Double](cardinalityName)

        var tmp = TrueDataType.getDataTypes(raw, colName, cardinality)

        val attTypeDF = tmp.select("dataType").groupBy("dataType").agg( expr(s"count(*) as cnt"))
          .orderBy(desc("cnt"))

        if (attTypeDF.limit(1).collect().length > 0) {
          df = df.withColumn("dataType",
            lit(attTypeDF.limit(1).collect()(0).getAs[String]("dataType")))
        } else {
          df = df.withColumn("dataType",
            lit("IsNull"))
        }

        val attMap = attTypeDF.collect
          .map(r => {
            r.getAs[String]("dataType") -> r.getAs[Long]("cnt")
          }).toMap

        var sizeDistinct = attMap.foldLeft(0.0)( _ + _._2)
        sizeDistinct = if (sizeDistinct == 0) 1 else sizeDistinct

        typesList.map { case (key, colname) =>
          df = df.withColumn(colname,
            lit(attMap.getOrElse(key, 0).toString.toDouble / sizeDistinct))
          df
        }

        val attSpecificTypeDF = tmp.select("specificType").groupBy("specificType")
          .agg( expr(s"count(*) as cnt")).orderBy(desc("cnt"))

        if (attSpecificTypeDF.limit(1).collect().length > 0) {
          df = df.withColumn("specificType",
            lit(attSpecificTypeDF.limit(1).collect()(0).getAs[String]("specificType")))
        } else {
          df = df.withColumn("specificType",
            lit("isNull"))
        }


        val attMapSpecificType = attSpecificTypeDF.collect.map(r => {

          r.getAs[String]("specificType") -> r.getAs[Long]("cnt")
        }).toMap
        sizeDistinct = attMapSpecificType.foldLeft(0.0)(_ + _._2)
        sizeDistinct = if (sizeDistinct == 0) 1 else sizeDistinct

        //        attSpecificTypeDF.show
        specifyTypesList.map{case (key, colname) =>
          df = df.withColumn(colname,
            lit(attMapSpecificType.getOrElse(key, 0).toString.toDouble / sizeDistinct))
          df
        }
        df



      case FreqWord.name =>
        val renamedCol = s"${colName}_raw"

        val topk = raw.select(lower(trim(col(colName))).as(renamedCol)).na.drop
          //          .groupBy(renamedCol).agg( expr(s"count(*) as ${colName.name}").as(colName.name))
          .filter(col(renamedCol) =!= " ")
          .groupBy(renamedCol).agg( expr(s"count(*)").as(colName))
          .orderBy(desc(colName)).limit(numberKWords).collect()
          .map(r => r.getAs[String](renamedCol)).toList

        df = df.withColumn(FreqWord.nameAtt, typedLit(topk))

        df
      case FreqWordSoundex.name =>
        val renamedCol = s"${colName}_raw"

        val topk = raw.select(soundex(trim(col(colName))).as(renamedCol)).na.drop
          .filter(col(renamedCol) =!= " ")
          .groupBy(renamedCol).agg( expr(s"count(*)").as(colName))
          .orderBy(desc(colName)).limit(numberKWords).collect()
          .map(r => r.getAs[String](renamedCol)).toList

        df = df.withColumn(FreqWordSoundex.nameAtt, typedLit(topk))
        df
    })
    df
  }




//
//  def main(args: Array[String]): Unit = {
//
//
//    val spark = SparkSession
//      .builder()
//      .master("local[*]")
//      .config("spark.driver.memory", "9g")
//      .config("spark.driver.maxResultSize", "9g")
//      .getOrCreate()
//    spark.sparkContext.setLogLevel("ERROR")
//    import spark.implicits._
////    val someDF = Seq(
////      ("ja", "bat", 4),
////      ("oscar", "mouse", 5),
////      ("carlos", "mouse", 5),
////      ("sergi", "horse", 6),
////      ("mike", "horse", 6),
////      ("juqn", "horse", 6)
////    ).toDF("person", "word", "n2")
//
//
//    val someDF =     spark.read
//      .option("header", "true").option("inferSchema", "true").option("delimiter",",")
//      .csv("/Users/javierflores/Documents/UPC/2021/published/nextiajd/src/main/resources/00_Emma_dataset.csv")
//
//    val someDF2 =     spark.read
//      .option("header", "true").option("inferSchema", "true").option("delimiter",";")
//      .csv("/Users/javierflores/Documents/UPC/2021/published/nextiajd/src/main/resources/querydataset.csv")
//
//    val someDF3 =     spark.read
//      .option("header", "true").option("inferSchema", "true").option("delimiter",",")
//      .csv("/Users/javierflores/Documents/UPC/2021/published/nextiajd/src/main/resources/PlanSales.csv")
//
//    val someDF4 =     spark.read
//      .option("header", "true").option("inferSchema", "true").option("delimiter",",")
//      .csv("/Users/javierflores/Documents/UPC/2021/published/nextiajd/src/main/resources/lista_comunidades_es.csv")
//
//
//
////    attributeProfile(someDF).show()
//    discovery(someDF, Seq(someDF2, someDF3, someDF4), "Capital", showAll = true).show()
////type,  film_director, cast
////    val colname = "film_director"
////    val freq = getFrequencyDF(someDF.select( trim(col(colname)).as(colname) ), colname )
////      freq.show()
////
////    freq.select( stddev_pop( col(colname).divide(lit(nrows)) ).as(s"${colname}_javi")  ).show
////
//
//
//
////
////    time { computeStringProfile(someDF) }
//
//
//
//  }

}




