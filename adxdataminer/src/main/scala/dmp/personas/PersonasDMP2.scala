package dmp.personas

import dmp.utils.tags_utils._
import dmp.utils.{LoadRuleData, SessionUtil, UserUtil}
import org.apache.commons.httpclient.HttpClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Created by yjl
  * Data: 2019/3/2
  * Time: 17:09
  */
object PersonasDMP2 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SessionUtil.getSpark
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //加载处理好的日志文件
    val df = spark.read.parquet("f:/mrdata/dmpout")
    //加载敏感词文件
    val stopwd_Map = LoadRuleData.loadStopwords
    val stopwd_BC = spark.sparkContext.broadcast(stopwd_Map)
    //加载app_dic文件
    val app_dicMap: mutable.HashMap[String, String] = LoadRuleData.loadAppDic
    val app_dicBC = spark.sparkContext.broadcast(app_dicMap)
    //过滤出符合用户标识的数据
    val filter_df = df.filter(" imei !='' or idfa !='' or mac !='' or androidid !='' or openudid !='' " +
      "or imeimd5 !='' or idfamd5 !='' or macmd5 !='' or androididmd5 !='' or openudidmd5 !='' " +
      "or imeisha1 !='' or idfasha1 !='' or macsha1 !='' or androididsha1 !='' or openudidsha1 !='' ")
      //过滤掉 appid和appname同时为空的数据
      .filter(" cast(longtitute as double)> -180 and cast(longtitute as double )< 180 and cast(lat as double)> -90 and cast(lat as double)< 90 and ( appname !='' or appid != '')")
      //把需求字段处理出来,方便使用
      .select('imei, 'idfa, 'mac, 'androidid, 'openudid,
      'imeimd5, 'idfamd5, 'macmd5, 'androididmd5, 'openudidmd5,
      'imeisha1, 'idfasha1, 'macsha1, 'androididsha1, 'openudidsha1,
      'adspacetype, 'appname, 'appid, 'adplatformproviderid, 'client, 'networkmannerid,
      'ispid, 'provincename, 'cityname, 'keywords, 'longtitute, 'lat)
    var i:Int=0
    val tagsRDD = filter_df.rdd.mapPartitions(iter => {
      val jedis = new Jedis("lu2", 6379)
      val httpClient = new HttpClient()
      iter.map(tp => {
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
        //广告标签
        val adTagList = AdTagsUtil.getTags(tp)
        //地域标签
        val areaTagList = AreaTagsUtil.getTags(tp)
        //客户端标签
        val app_dicMap = app_dicBC.value
        val clientTagsList = ClientTagsUtil.getTags(tp, app_dicMap)
        //关键字标签
        val stopwd_Map = stopwd_BC.value
        val keywordsTagsList = KeywordTagsUtil.getTags(tp, stopwd_Map)
        //商圈标签
        val busiTagsList = BuisTagsUtil.getTags(tp, jedis, httpClient)
        i+=1
        println(i)
        (userId, adTagList ++ areaTagList ++ clientTagsList ++ keywordsTagsList ++ busiTagsList)
      })
    })
    println(tagsRDD.count())
    val resRDD = tagsRDD.reduceByKey((ls1, ls2) => {
      (ls1 ++ ls2).groupBy(_._1).mapValues(_.size).toList
    })
    val resDF = resRDD.map(tp => TargsBean(tp._1, tp._2)).toDF()
    resDF.show(100, false)
    println(resDF.count())
    val conf = new Configuration
    val path = new Path("f:dmp/jsonout/person1")
    val fs = FileSystem.get(conf)
    if (fs.exists(path)) {
      println("文件夹存在")
      fs.delete(path, true)
      println("删除已有文件夹")
    }
    resDF.coalesce(1).write.json("f:dmp/jsonout/person1")
    spark.close()
  }

}
