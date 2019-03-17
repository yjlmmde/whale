package dmp.utils

import org.apache.commons.lang3.StringUtils

/**
  * Created by yjl
  * Data: 2019/3/2
  * Time: 21:45
  */
object UserUtil {
  //递归根据字段级别识别userid
  def getUid(arr: Array[String]): String = {
    var tmp = ""
    for (elem <- arr) {

      if (StringUtils.isNotEmpty(elem)) {
        tmp = elem
        return tmp
      }
    }
    tmp
  }

}
