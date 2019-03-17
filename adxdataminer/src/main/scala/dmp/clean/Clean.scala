package dmp.clean

import dmp.utils.{CleanUtil, SessionUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._

/**
  * Created by yjl
  * Data: 2019/2/26
  * Time: 14:58
  */
object Clean {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org").setLevel(Level.ERROR)
    //伪装用户身份,赋予root用户权限
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark: SparkSession = SessionUtil.getSpark
    //从hdfs读取原始压缩文件
    val ds: Dataset[String] = spark.read.textFile("hdfs://lu2:9000/dmpdata/input")
    //转成rdd 过滤芯出长度 >= 85 的数据
    val ds_Filter: RDD[Array[String]] = ds.rdd.map(_.split(",")).filter(tp => tp.length >= 85)
    //过滤掉无法转成 Int Double 的脏数据
    //调用封装好的工具类
    val judge_RDD: RDD[Array[String]] = ds_Filter.filter(CleanUtil.Judge(_))
    //封装schema信息
    val schema: StructType = new StructType(Array(
      new StructField("sessionid", StringType),
      new StructField("advertisersid", IntegerType),
      new StructField("adorderid", IntegerType),
      new StructField("adcreativeid", IntegerType),
      new StructField("adplatformproviderid", IntegerType),
      new StructField("sdkversion", StringType),
      new StructField("adplatformkey", StringType),
      new StructField("putinmodeltype", IntegerType),
      new StructField("requestmode", IntegerType),
      new StructField("adprice", DoubleType),
      new StructField("adppprice", DoubleType),
      new StructField("requestdate", StringType),
      new StructField("ip", StringType),
      new StructField("appid", StringType),
      new StructField("appname", StringType),
      new StructField("uuid", StringType),
      new StructField("device", StringType),
      new StructField("client", IntegerType),
      new StructField("osversion", StringType),
      new StructField("density", StringType),
      new StructField("pw", IntegerType),
      new StructField("ph", IntegerType),
      new StructField("long", StringType),
      new StructField("lat", StringType),
      new StructField("provincename", StringType),
      new StructField("cityname", StringType),
      new StructField("ispid", IntegerType),
      new StructField("ispname", StringType),
      new StructField("networkmannerid", IntegerType),
      new StructField("networkmannername", StringType),
      new StructField("iseffective", IntegerType),
      new StructField("isbilling", IntegerType),
      new StructField("adspacetype", IntegerType),
      new StructField("adspacetypename", StringType),
      new StructField("devicetype", IntegerType),
      new StructField("processnode", IntegerType),
      new StructField("apptype", IntegerType),
      new StructField("district", StringType),
      new StructField("paymode", IntegerType),
      new StructField("isbid", IntegerType),
      new StructField("bidprice", DoubleType),
      new StructField("winprice", DoubleType),
      new StructField("iswin", IntegerType),
      new StructField("cur", StringType),
      new StructField("rate", DoubleType),
      new StructField("cnywinprice", DoubleType),
      new StructField("imei", StringType),
      new StructField("mac", StringType),
      new StructField("idfa", StringType),
      new StructField("openudid", StringType),
      new StructField("androidid", StringType),
      new StructField("rtbprovince", StringType),
      new StructField("rtbcity", StringType),
      new StructField("rtbdistrict", StringType),
      new StructField("rtbstreet", StringType),
      new StructField("storeurl", StringType),
      new StructField("realip", StringType),
      new StructField("isqualityapp", IntegerType),
      new StructField("bidfloor", DoubleType),
      new StructField("aw", IntegerType),
      new StructField("ah", IntegerType),
      new StructField("imeimd5", StringType),
      new StructField("macmd5", StringType),
      new StructField("idfamd5", StringType),
      new StructField("openudidmd5", StringType),
      new StructField("androididmd5", StringType),
      new StructField("imeisha1", StringType),
      new StructField("macsha1", StringType),
      new StructField("idfasha1", StringType),
      new StructField("openudidsha1", StringType),
      new StructField("androididsha1", StringType),
      new StructField("uuidunknow", StringType),
      new StructField("userid", StringType),
      new StructField("iptype", IntegerType),
      new StructField("initbidprice", DoubleType),
      new StructField("adpayment", DoubleType),
      new StructField("agentrate", DoubleType),
      new StructField("lrate", DoubleType),
      new StructField("adxrate", DoubleType),
      new StructField("title", StringType),
      new StructField("keywords", StringType),
      new StructField("tagid", StringType),
      new StructField("callbackdate", StringType),
      new StructField("channelid", StringType),
      new StructField("mediatype", IntegerType))
    )
    //rdd[String] --> rdd[Row]
    val row_RDD: RDD[Row] = judge_RDD.map(arr => {
      Row(
        arr(0),
        arr(1).toInt,
        arr(2).toInt,
        arr(3).toInt,
        arr(4).toInt,
        arr(5),
        arr(6),
        arr(7).toInt,
        arr(8).toInt,
        arr(9).toDouble,
        arr(10).toDouble,
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        arr(17).toInt,
        arr(18),
        arr(19),
        arr(20).toInt,
        arr(21).toInt,
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        arr(26).toInt,
        arr(27),
        arr(28).toInt,
        arr(29),
        arr(30).toInt,
        arr(31).toInt,
        arr(32).toInt,
        arr(33),
        arr(34).toInt,
        arr(35).toInt,
        arr(36).toInt,
        arr(37),
        arr(38).toInt,
        arr(39).toInt,
        arr(40).toDouble,
        arr(41).toDouble,
        arr(42).toInt,
        arr(43),
        arr(44).toDouble,
        arr(45).toDouble,
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        arr(57).toInt,
        arr(58).toDouble,
        arr(59).toInt,
        arr(60).toInt,
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        arr(73).toInt,
        arr(74).toDouble,
        arr(75).toDouble,
        arr(76).toDouble,
        arr(77).toDouble,
        arr(78).toDouble,
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        arr(84).toInt)
    })
    val df: DataFrame = spark.createDataFrame(row_RDD,schema)
    //将过滤好的数据 写入到hdfs  格式为parquet
    df.coalesce(1).write.parquet("hdfs://lu2:9000/dmpdata/outparquet")
    spark.close()

  }
}

