package shop.clean

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}


/**
  * Created by yjl
  * Data: 2019/3/15
  * Time: 20:44
  */
object CleanDemo1 {
  //屏蔽日志
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //创建spark对象
    val spark = SparkSession.builder()
      .appName("shopdata")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val ds = spark.read.textFile("F:\\shopdata\\input")
    val ds1 = ds.filter(!_.isEmpty).map(tp => {
      val arrs = tp.split(" ")
      val js = arrs(3)
      var obj = new JSONObject()
      try {
        obj = JSON.parseObject(js)
        val u = obj.getJSONObject("u")
        u.remove("email")
        u.remove("phoneNbr")
        u.remove("birthday")
        u.remove("isRegistered")
        u.remove("isLogin")
        u.remove("addr")
        u.remove("gender")
      } catch {
        case e:Exception =>
      }

      String.valueOf(obj)
    })
    val resds = ds1.filter(!_.isEmpty)
    resds.show(10, false)
    val conf = new Configuration()
    val path = new Path("F:\\shopdata\\output")
    val fs =FileSystem.get(conf)
    if (fs.exists(path)){
      println("文件夹已存在")
      fs.delete(path,true)
      println("文件夹已删除")
    }
    resds.coalesce(1).write.json("F:\\shopdata\\output")
    spark.close()
  }
}
