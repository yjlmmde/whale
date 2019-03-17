package dmp.utils.tags_utils

import org.apache.spark.sql.Row

/**
  * Created by yjl
  * Data: 2019/3/3
  * Time: 20:15
  */
trait TagsUtil {
  def getTags(tp:Row):List[(String,Int)]
}

