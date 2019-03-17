package dmp.area

import java.util.Properties

import com.typesafe.config.ConfigFactory
import dmp.config.DMPConfig
import dmp.utils.SessionUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * Created by yjl
  * Data: 2019/2/26
  * Time: 23:14
  */
object DMP1 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //伪装用户身份
  System.setProperty("HADOOP_USER_NAME", "root")
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SessionUtil.getSpark
    //导入隐式转换
    import spark.implicits._
    val df: DataFrame = spark.read.parquet("hdfs://lu2:9000/dmpdata/outparquet/")

    val res: DataFrame = df.groupBy("provincename","cityname").count()
    res.show(10)
    df.printSchema()
    df.createTempView("province_city")
   /* val last= spark.sql("select provincename,cityname,count(*) " +
      "from province_city groupby provincename,cityname")
    last.show()*/
    //写入到数据库
    res.write.mode(SaveMode.Overwrite).jdbc(DMPConfig.url,"pro_city_ov",DMPConfig.prop)
    //转为json格式存储
    res.write.json("hdfs://lu2:9000/dmpdata/outjson")
    spark.close()
  }
}
