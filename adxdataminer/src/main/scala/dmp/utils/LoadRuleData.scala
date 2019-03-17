package dmp.utils

import scala.collection.mutable
import scala.io.Source

/**
  * Created by yjl
  * Data: 2019/3/3
  * Time: 21:23
  */
object LoadRuleData {
  //加载 appname字典文件
  def loadAppDic = {
    //加载 appname字典文件
    val lines = Source.fromFile("F:\\dmp/app_dict.txt").getLines()
    val app_dicMap = new mutable.HashMap[String, String]()
    for (elem <- lines) {
      val arr = elem.split("\t")
      val appname = arr(1)
      val appid = arr(4)
      app_dicMap.put(appid, appname)
    }
    app_dicMap
  }

  //加载敏感词文件
  def loadStopwords = {
    //加载敏感词文件
    val stopwords = Source.fromFile("F:\\dmp/stopwords.txt", "utf-8").getLines()
    val stopwd_Map = new mutable.HashMap[String, Null]()
    //将敏感词存放大Map中
    for (elem <- stopwords) {
      val arrs = elem.split(" ")
      for (i <- arrs) {
        stopwd_Map.put(i, null)
      }
    }
    stopwd_Map
  }

}
