package dmp.map

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{HttpClient, HttpMethod}


/**
  * Created by yjl
  * Data: 2019/3/4
  * Time: 0:02
  */
object MapFroInet {
  //81021fb3005f791ccf369b179286c522
  //https://restapi.amap.com/v3/geocode/regeo?parameters
  def main(args: Array[String]): Unit = {
    val method = new GetMethod("https://www.sina.cn/")
    val client = new HttpClient()
    val http_status = client.executeMethod(method)
    println(http_status)
    val body = method.getResponseBodyAsString()
    val s1 = JSON.parseObject(method.getResponseBodyAsString())
    println(s1  )

    method.releaseConnection()

  }

}
