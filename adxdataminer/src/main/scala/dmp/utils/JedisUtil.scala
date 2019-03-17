package dmp.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by yjl
  * Data: 2019/3/1
  * Time: 17:36
  * redsi
  */
object JedisUtil {
  def getPool(host: String, port: Int) = {
    //获取连接池配置对对象
    val config: JedisPoolConfig = new JedisPoolConfig
    //设置最大连接池数
    config.setMaxTotal(2000)
    //创建连接池对象
    lazy val pool: JedisPool = new JedisPool(config, host, port)
    pool
  }

  def getJedis(host: String, port: Int)=getPool(host,port).getResource
}
