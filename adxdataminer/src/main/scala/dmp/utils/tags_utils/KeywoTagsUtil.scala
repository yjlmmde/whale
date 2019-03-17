package dmp.utils.tags_utils

import org.apache.spark.sql.Row

import scala.collection.mutable


/**
  * Created by yjl
  * Data: 2019/3/3
  * Time: 20:34
  */
object KeywordTagsUtil  {
  def getTags(tp: Row,stopmap:mutable.HashMap[String,Null])={
    var tags = List[(String, Int)]()
    val keywords = tp.getAs[String]("keywords")
    //将关键词标签放到List中
    keywords.split("\\|").filter(_.size > 2).filter(!stopmap.contains(_)).map(tp => tags :+= ("K" + tp, 1))
    tags
  }

}
