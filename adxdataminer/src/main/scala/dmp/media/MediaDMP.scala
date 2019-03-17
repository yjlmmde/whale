package dmp.media

import dmp.utils.{FlagsUtil, SessionUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

import scala.collection.mutable

/**
  * Created by yjl
  * Data: 2019/2/27
  * Time: 23:44
  */
object MediaDML {
  //屏蔽日志
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SessionUtil.getSpark
    import spark.implicits._
    val ruledata: Dataset[String] = spark.read.textFile("f:/dmp/")
    val dmldf: DataFrame = spark.read.parquet("f:/mrdata/dmpout")
    val map: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    val ruledr = ruledata.rdd.map(tp => {
      val arr: Array[String] = tp.split("\t")
      val appname = arr(1)
      val appid = arr(4)
      map.put(appid, appname)
    })
    //将规则数据广播出去,到每一个task
    val br = spark.sparkContext.broadcast(map)
    //过滤掉脏数据
    val filter_rdd = dmldf.filter(tp => {
      !tp.getAs[String]("appid").isEmpty || !tp.getAs[String]("appid").isEmpty
    })
      .rdd.map(tp => {//转RDD进行下一步的字段匹配
      //天
      val day: String = tp.getAs[String]("requestdate").substring(0, 10)
      //appid
      val appid: String = tp.getAs[String]("appid")
      //appname
      var appname: String = tp.getAs[String]("appname")
      //使用scala自带工具类进行字段的判断
      if (StringUtils.isEmpty(appname)) {
        //获取广播变量进行模式匹配
        br.value.get(appid) match {
          case Some(i) => appname = i
          case None => appname = appid
        }
      }
      //使用封装好的工具类获取flag标识指标
      val flag: List[Double] = FlagsUtil.getFlags(tp)
      ((day, appname), flag)
    })
    val result_rdd = filter_rdd.reduceByKey((ls1, ls2) => {
      ls1.zip(ls2).map(tp => tp._1 + tp._2)
    }).map(tp => {
      //使用样例类封装数据
      MediaBean(tp._1._1, tp._1._2, tp._2(0).toInt, tp._2(1).toInt, tp._2(2).toInt, tp._2(3).toInt, tp._2(4).toInt, tp._2(5).toInt, tp._2(6).toInt, tp._2(7), tp._2(8))
    })
    result_rdd.toDF().show(22)
    println(result_rdd.toDF().count())
    /*
        StringUtils.isEmpty(appnamde){
          appid
          StringUtils.isNotEmpty(appid){
            gdgey
          }else "位于德国"
        }
    */
    spark.close()

  }

}

//样例类封装schema信息
case class MediaBean(day: String, appname: String, orig_req: Int, val_req: Int, adv_req: Int, bidding: Int, bid_succ: Int, adv_display: Int, adv_cli: Int, adv_consum: Double, adv_costs: Double)
