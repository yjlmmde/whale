package dmp.config

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by yjl
  * Data: 2019/3/1
  * Time: 17:22
  */
object DMPConfig {
  //获取配置文件对象
  lazy val conf = ConfigFactory.load()
  val host: String = conf.getString("redis.host")
  val port: Int = conf.getInt("redis.port")

  val url: String = conf.getString("jdbc.url")
  val user: String = conf.getString("jdbs.user")
  val password: String = conf.getString("jdbc.password")
  //创建prop实例
  val prop: Properties = new Properties()
  prop.setProperty("user", user)
  prop.setProperty("password", password)



}
