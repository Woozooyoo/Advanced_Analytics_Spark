/*
 * Copyright 2015 and onwards Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */
package intro

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._ // for lit(), first(), etc.

case class MatchData(
  id_1: Int,
  id_2: Int,
  cmp_fname_c1: Option[Double],
  cmp_fname_c2: Option[Double],
  cmp_lname_c1: Option[Double],
  cmp_lname_c2: Option[Double],
  cmp_sex: Option[Int],
  cmp_bd: Option[Int],
  cmp_bm: Option[Int],
  cmp_by: Option[Int],
  cmp_plz: Option[Int],
  is_match: Boolean
)

object RunIntro extends Serializable {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Intro").setMaster("local[*]")
    //创建Spark的对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val preview = spark.read.csv("./ch02-intro/linkage")
    preview.show()
    preview.printSchema()

    val parsed = spark.read
      .option("header", "true")
      .option("nullValue", "?")
      .option("inferSchema", "true")
      .csv("./ch02-intro/linkage")
    parsed.show()
    parsed.printSchema()

    println(parsed.count())
    parsed.cache()

    parsed.groupBy("is_match").count().orderBy($"count".desc).show()
    //两个count等同
    parsed.createOrReplaceTempView("linkage")
    spark.sql("""
      SELECT is_match, COUNT(*) cnt
      FROM linkage
      GROUP BY is_match
      ORDER BY cnt DESC
    """).show()

    val summary = parsed.describe() //计算所有 数值列 所有非空值的count，mean，stddev，min，max
    summary.show()
    summary.select("summary", "cmp_fname_c1", "cmp_fname_c2").show()

    val matches = parsed.where("is_match = true")
    val misses = parsed.filter($"is_match" === false)
    val matchSummary = matches.describe()//计算match表的 数值列 所有非空值的count，mean，stddev，min，max
    val missSummary = misses.describe() //计算notMatch表的 数值列 所有非空值的count，mean，stddev，min，max

    val matchSummaryT = pivotSummary(matchSummary)  //转置 概要表
    val missSummaryT = pivotSummary(missSummary)
    matchSummaryT.createOrReplaceTempView("match_desc")
    missSummaryT.createOrReplaceTempView("miss_desc")
    spark.sql("""
      SELECT 
          a.field, a.count + b.count total, a.mean - b.mean delta 
      FROM
          match_desc a 
          INNER JOIN miss_desc b 
              ON a.field = b.field 
      ORDER BY delta DESC, total DESC 
    """).show()

    val matchData = parsed.as[MatchData]
    val scored = matchData.map { md =>
      (scoreMatchData(md), md.is_match)
    }.toDF("score", "is_match")
    crossTabs(scored, 4.0).show()
  }

  def crossTabs(scored: DataFrame, t: Double): DataFrame = {
    scored.
      selectExpr(s"score >= $t as above", "is_match").
      groupBy("above").
      pivot("is_match", Seq("true", "false")).
      count()
  }

  case class Score(value: Double) {
    def +(oi: Option[Int]) = {
      Score(value + oi.getOrElse(0))
    }
  }

  def scoreMatchData(md: MatchData): Double = {
    (Score(md.cmp_lname_c1.getOrElse(0.0)) + md.cmp_plz +
        md.cmp_by + md.cmp_bd + md.cmp_bm).value
  }

  def pivotSummary(desc: DataFrame): DataFrame = {
    val lf = longForm(desc)
    lf.groupBy("field")
      .pivot("metric", Seq("count", "mean", "stddev", "min", "max"))//转置 需要知道metric列里有什么变量名
      .agg(first("value"))
  }

  def longForm(desc: DataFrame): DataFrame = {
    import desc.sparkSession.implicits._ // For toDF RDD -> DataFrame conversion
    val schema = desc.schema
    desc.flatMap(row => {
      val metric = row.getString(0) //度量标准(count,mean...)
      (1 until row.size).map(i => (metric, schema(i).name, row.getString(i).toDouble))
    })//这个是DataSet专成DF(即DataSet[Row])
    .toDF("metric", "field", "value")
  }
}
