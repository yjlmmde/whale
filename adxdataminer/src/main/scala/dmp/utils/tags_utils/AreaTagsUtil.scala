package dmp.utils.tags_utils

import org.apache.spark.sql.Row

/**
  * Created by yjl
  * Data: 2019/3/3
  * Time: 20:22
  */
object AreaTagsUtil {
   def getTags(tp: Row): List[(String, Int)] = {
    var tags = List[(String, Int)]()
    val provincename = tp.getAs[String]("provincename")
    val cityname = tp.getAs[String]("cityname")
    tags :+= ("ZP" + provincename, 1)
    tags :+= ("ZC" + cityname, 1)
    tags
  }
}
