package dmp.utils.tags_utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * Created by yjl
  * Data: 2019/3/3
  * Time: 20:24
  */
object ClientTagsUtil  {

  def getTags(tp: Row, app_dicBC: mutable.HashMap[String, String]) = {
    var tags = List[(String, Int)]()
    var appname = tp.getAs[String]("appname")
    var appid = tp.getAs[String]("appid")
    val client = tp.getAs[Int]("client")
    val networkmannerid = tp.getAs[Int]("networkmannerid")
    val ispid = tp.getAs[Int]("ispid")
    //字典数据匹配appname
    val dic_Appname = app_dicBC.get(appid)
    if (StringUtils.isEmpty(appname)) {
      dic_Appname match {
        case Some(i) => appname = i
        case None => appname = appid
      }
    }
    tags :+= ("APP" + dic_Appname, 1)

    //抽取操作系统信息
    tags :+= ("D0001000" + client, 1)

    //抽取网络类型信息
    tags :+= ("D0002000" + networkmannerid, 1)

    //抽取运营商类型
    tags :+= ("D0003000" + ispid, 1)
    tags
  }
}
