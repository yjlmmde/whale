package dmp.utils.tags_utils

import org.apache.spark.sql.Row

/**
  * Created by yjl
  * Data: 2019/3/3
  * Time: 20:18
  */
object AdTagsUtil {
   def getTags(tp: Row): List[(String, Int)] = {
    var tags = List[(String, Int)]()
    val adspacetype = tp.getAs[Int]("adspacetype")
    val adplatformproviderid = tp.getAs[Int]("adplatformproviderid")
    //抽取广告位类型信息
    if (adspacetype < 10) {
      tags :+= ("LC0" + adspacetype, 1)
    } else {
      tags :+= ("LC" + adspacetype, 1)
    }
    //抽取运营商类型
    tags :+= ("CN" + adplatformproviderid, 1)

    tags
  }
}
