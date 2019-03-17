package dmp.personas

import dmp.utils.{LoadRuleData, SessionUtil, UserUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.io.Source


/**
  * Created by yjl
  * Data: 2019/3/2
  * Time: 10:26
  * 数据清洗
  */
object PersonasDMP {
  //屏蔽日志
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SessionUtil.getSpark
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //加载处理好的日志文件
    val df = spark.read.parquet("f:/mrdata/dmpout")
    val stopwd_Map: mutable.HashMap[String, Null] = LoadRuleData.loadStopwords
    val stopwd_BC = spark.sparkContext.broadcast(stopwd_Map)
    val app_dicMap: mutable.HashMap[String, String] = LoadRuleData.loadAppDic
    val app_dicBC = spark.sparkContext.broadcast(app_dicMap)
    //过滤出符合用户标识的数据
    val filter_df = df.filter(" imei !='' or idfa !='' or mac !='' or androidid !='' or openudid !='' " +
      "or imeimd5 !='' or idfamd5 !='' or macmd5 !='' or androididmd5 !='' or openudidmd5 !='' " +
      "or imeisha1 !='' or idfasha1 !='' or macsha1 !='' or androididsha1 !='' or openudidsha1 !='' ")
      //过滤掉 appid和appname同时为空的数据
      .filter("appname !='' or appid != ''")
      //把需求字段处理出来,方便使用
      .select('imei, 'idfa, 'mac, 'androidid, 'openudid,
      'imeimd5, 'idfamd5, 'macmd5, 'androididmd5, 'openudidmd5,
      'imeisha1, 'idfasha1, 'macsha1, 'androididsha1, 'openudidsha1,
      'adspacetype, 'appname, 'appid, 'adplatformproviderid, 'client, 'networkmannerid,
      'ispid, 'provincename, 'cityname, 'keywords, 'longtitute, 'lat)

    val tagsRDD = filter_df.rdd.map(tp => {
      var appname = tp.getAs[String]("appname")
      var appid = tp.getAs[String]("appid")
      val adspacetype = tp.getAs[Int]("adspacetype")
      val adplatformproviderid = tp.getAs[Int]("adplatformproviderid")
      val client = tp.getAs[Int]("client")
      val networkmannerid = tp.getAs[Int]("networkmannerid")
      val ispid = tp.getAs[Int]("ispid")
      val provincename = tp.getAs[String]("provincename")
      val cityname = tp.getAs[String]("cityname")
      val keywords = tp.getAs[String]("keywords")
      val longtitute = tp.getAs[String]("longtitute")
      val lat = tp.getAs[String]("lat")


      val imei = tp.getAs[String]("imei")
      val idfa = tp.getAs[String]("idfa")
      val mac = tp.getAs[String]("mac")
      val androidid = tp.getAs[String]("androidid")
      val openudid = tp.getAs[String]("openudid")
      val imeimd5 = tp.getAs[String]("imeimd5")
      val idfamd5 = tp.getAs[String]("idfamd5")
      val macmd5 = tp.getAs[String]("macmd5")
      val androididmd5 = tp.getAs[String]("androididmd5")
      val openudidmd5 = tp.getAs[String]("openudidmd5")
      val imeisha1 = tp.getAs[String]("imeisha1")
      val idfasha1 = tp.getAs[String]("idfasha1")
      val macsha1 = tp.getAs[String]("macsha1")
      val androididsha1 = tp.getAs[String]("androididsha1")
      val openudidsha1 = tp.getAs[String]("openudidsha1")
      val judge_arr: Array[String] = Array(imei, idfa, mac, androidid, openudid, imeimd5, idfamd5, macmd5, androididmd5, openudidmd5, imeisha1, idfasha1,
        macsha1, androididsha1, openudidsha1)
      //使用工具类抽取用户标识
      val userId = UserUtil.getUid(judge_arr)

      //抽取广告位类型信息
      val adspaTuple = if (adspacetype < 10) {
        ("LC0" + adspacetype, 1)
      } else {
        ("LC" + adspacetype, 1)
      }

      //字典数据匹配appname
      val dic_Appname = app_dicBC.value.get(appid)
      if (StringUtils.isEmpty(appname)) {
        dic_Appname match {
          case Some(i) => appname = i
          case None => appname = appid
        }
      }
      val app_Tuple = ("APP" + dic_Appname, 1)

      //抽取渠道信息
      val adplatTuple = ("CN" + adplatformproviderid, 1)

      //抽取操作系统信息
      val clientTuple = ("D0001000" + client, 1)

      //抽取网络类型信息
      val networkTuple = ("D0002000" + networkmannerid, 1)

      //抽取运营商类型
      val ispidTuple = ("D0003000" + ispid, 1)

      //抽取地域标签
      val proTuple = ("ZP" + provincename, 1)
      val cityTuple = ("ZC" + cityname, 1)

      //抽取关键字标签
      val stopmap = stopwd_BC.value
      var keyList = List[(String, Int)]()
      //将关键词标签放到List中
      keywords.split("\\|").filter(_.size > 2).filter(!stopmap.contains(_)).map(tp => keyList :+= ("K" + tp, 1))
      //商圈标签


      //将各种标签装到FlagList
      var flagList = List[(String, Int)]()
      flagList :+= adspaTuple
      flagList :+= app_Tuple
      flagList :+= adplatTuple
      flagList :+= clientTuple
      flagList :+= networkTuple
      flagList :+= proTuple
      flagList :+= cityTuple


      (userId, flagList ++ keyList)
    })
    val resRDD = tagsRDD.reduceByKey((ls1, ls2) => {
      //对list元祖的 _.1 进行分组,计算分组后的value值list的长度
      (ls1 ++ ls2).groupBy(_._1).mapValues(_.size).toList
    })
    val resDF = resRDD.map(tp => {
      TargsBean(tp._1, tp._2)
    }).toDF()
    println(resRDD.count())
    resDF.show(100, false)
    println(resDF.count())
    resDF.coalesce(1).write.json("f:dmp/jsonout/person2")
    spark.close()
  }
}

case class TargsBean(userId: String, targs: List[(String, Int)])
