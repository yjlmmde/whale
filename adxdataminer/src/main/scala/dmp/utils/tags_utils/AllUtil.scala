package dmp.utils.tags_utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Created by yjl
  * Data: 2019/3/3
  * Time: 22:50
  */
object AllUtil {
  def getTags(tp:Row)={

    val adspacetype = tp.getAs[Int]("adspacetype")
    val adplatformproviderid = tp.getAs[Int]("adplatformproviderid")
    val client = tp.getAs[Int]("client")
    val networkmannerid = tp.getAs[Int]("networkmannerid")
    val ispid = tp.getAs[Int]("ispid")
    val provincename = tp.getAs[String]("provincename")
    val cityname = tp.getAs[String]("cityname")

    val longtitute = tp.getAs[String]("longtitute")
    val lat = tp.getAs[String]("lat")
    //抽取广告位类型信息
    val adspaTuple = if (adspacetype < 10) {
      ("LC0" + adspacetype, 1)
    } else {
      ("LC" + adspacetype, 1)
    }


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


    //商圈标签

    //将各种标签装到FlagList
    var flagList = List[(String, Int)]()
    flagList :+= adspaTuple
    flagList :+= adplatTuple
    flagList :+= clientTuple
    flagList :+= networkTuple
    flagList :+= proTuple
    flagList :+= cityTuple

    flagList
  }

}
