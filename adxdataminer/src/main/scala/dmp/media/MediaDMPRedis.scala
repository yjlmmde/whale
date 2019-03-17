package dmp.media

import dmp.config.DMPConfig
import dmp.utils.{FlagsUtil, JedisUtil, SessionUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.{BufferedSource, Source}

/**
  * Created by yjl
  * Data: 2019/3/1
  * Time: 20:35
  * 使用redis存储字典文件字段
  */
object MediaDMPRedis {
  //屏蔽日志
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //获取spark对象
    val spark: SparkSession = SessionUtil.getSpark
    import spark.implicits._
    import scala.collection.JavaConversions._
    val df = spark.read.parquet("f:/mrdata/dmpout")
    val ds = df.select('requestdate, 'appname, 'appid, 'requestmode, 'processnode, 'iseffective, 'isbilling, 'isbid, 'iswin, 'winprice, 'adpayment, 'adorderid)
      .filter("appid != '' or appname != ''")
    val app_rdd = ds.rdd.mapPartitions(iter => {
      val jedis = JedisUtil.getJedis(DMPConfig.host, DMPConfig.port)
      val ree = iter.map(tp => {
        var appname = tp.getAs[String]("appname")
        val day = tp.getAs[String]("requestdate").substring(0, 10)
        val appid = tp.getAs[String]("appid")
        if (StringUtils.isEmpty("appname")) {
          //使用option包装一下
          val tmpappname = Option(jedis.hget("app_dic", appid))
          tmpappname match {
            case Some(i) => appname = i
            case None => appname = appid
          }
        }
        val flag = FlagsUtil.getFlags(tp)
        ((day, appname), flag)
      })
      jedis.close()
      ree
    })
    val filter_rdd = app_rdd.reduceByKey((ls1, ls2) => {
      ls1.zip(ls2).map(tp => tp._1 + tp._2)
    })
    val result= filter_rdd.map (tp=>{
      MediaBean(tp._1._1, tp._1._2, tp._2(0).toInt, tp._2(1).toInt, tp._2(2).toInt, tp._2(3).toInt, tp._2(4).toInt, tp._2(5).toInt, tp._2(6).toInt, tp._2(7), tp._2(8))
    })
    result.toDF().write.mode(SaveMode.Overwrite).jdbc(DMPConfig.url,"appname_jedis",DMPConfig.prop)
    spark.close()
  }
}
