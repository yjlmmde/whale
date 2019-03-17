package dmp.utils.tags_utils

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.JSON
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * Created by yjl
  * Data: 2019/3/4
  * Time: 20:16
  * 商圈工具
  * 先去本地智库匹配查询
  * 本地匹配不到,就去网上查询
  * 并将结果添加到本地知识库中
  */
object BuisTagsUtil {
  def getTags(tp: Row, jedis: Jedis, httpClient: HttpClient) = {
    var tagsList = new ListBuffer[(String, Int)]
    val longtitute = tp.getAs[String]("longtitute")
    val lat = tp.getAs[String]("lat")
    //蒋经纬度数据转换成gehashcode编码,先去本地商圈知识库匹配查询
    val geocode = GeoHash.withCharacterPrecision(lat.toDouble, longtitute.toDouble, 9).toBase32
    //如果本地查询不到,就去网上查询
    val bizInfor = jedis.hget("bizdic", geocode)

    if (bizInfor != null) {//本地知识库找得到
      val tmp = bizInfor.split(",").map(tp=>(tp,1)).toList
      tagsList ++ tmp
    } else {
      //本地知识库匹配不到,就去网络上查询
      val method = new GetMethod("https://restapi.amap.com/v3/geocode/regeo?key=81021fb3005f791ccf369b179286c522&location=" + longtitute + "," + lat)
      httpClient.executeMethod(method)
      val resJson = method.getResponseBodyAsString
      val rootJson = JSON.parseObject(resJson)

      val status = rootJson.getString("status")
      if ("1".equals(status)) {
        val busArr = rootJson.getJSONObject("regeocode").getJSONObject("addressComponent").getJSONArray("businessAreas")
        var tmpSb = new StringBuilder
        if (busArr != null && busArr.size()>0) {
          for (i <- 0 until busArr.size()) {
            val bizName= busArr.getJSONObject(i).getString("name")
            tmpSb.append(bizName)
            tagsList :+= ((bizName, 1))
          }
        }
        //将结果写到redis中释放资源
        jedis.hset("bizdic", geocode,tmpSb.mkString(","))
      }
      //释放http连接
      method.releaseConnection()
      tagsList
    }
  }

}
