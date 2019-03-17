package dmp.appdic

import dmp.config.DMPConfig
import dmp.utils.JedisUtil
import redis.clients.jedis.Jedis

import scala.io.Source

/**
  * Created by yjl
  * Data: 2019/3/1
  * Time: 21:10
  * 将字典数据从写入到Redis中
  */
object DictRedis {
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = JedisUtil.getJedis(DMPConfig.host, DMPConfig.port)
    val lines = Source.fromFile("f:/dmp/app_dict.txt").getLines()
    for (elem <- lines) {
      val arr: Array[String] = elem.split("\t")
      val appName = arr(1)
      val appId = arr(4)
      jedis.hset("app_dic",appId,appName)
    }
    jedis.close()
  }
}
