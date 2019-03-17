package dmp.clean

import dmp.utils.CleanUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by yjl
  * Data: 2019/2/26
  * Time: 22:39
  */
object Clean_CaseClass01 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DMP")
      .master("local[*]")
      .getOrCreate()
    //从hdfs读取原始压缩文件
    val ds: Dataset[String] = spark.read.textFile("f:/mrdata/dmpint")
    println("数据长度"+ds.count())
    //转成rdd 过滤芯出长度 >= 85 的数据                      //一直切,不管有没有数据
    val ds_Filter: RDD[Array[String]] = ds.rdd.map(_.split(",",-1)).filter(tp => tp.length >= 85)
    //过滤掉无法转成 Int Double 的脏数据
    //调用封装好的工具类
    println("长度符合数据"+ds_Filter.count())
    val judge_RDD: RDD[Array[String]] = ds_Filter.filter(CleanUtil.Judge(_))
    println("过滤之后数据"+judge_RDD.count())
    //封装成rdd[pojo]
    import dmp.utils.RichString._
    val res: RDD[DMP] = judge_RDD.map(arr => {
      DMP(
        arr(0),
        arr(1).toIntPlus,
        arr(2).toIntPlus,
        arr(3).toIntPlus,
        arr(4).toIntPlus,
        arr(5),
        arr(6),
        arr(7).toIntPlus,
        arr(8).toIntPlus,
        arr(9).toDoublePlus,
        arr(10).toDoublePlus,
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        arr(17).toIntPlus,
        arr(18),
        arr(19),
        arr(20).toIntPlus,
        arr(21).toIntPlus,
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        arr(26).toIntPlus,
        arr(27),
        arr(28).toIntPlus,
        arr(29),
        arr(30).toIntPlus,
        arr(31).toIntPlus,
        arr(32).toIntPlus,
        arr(33),
        arr(34).toIntPlus,
        arr(35).toIntPlus,
        arr(36).toIntPlus,
        arr(37),
        arr(38).toIntPlus,
        arr(39).toIntPlus,
        arr(40).toDoublePlus,
        arr(41).toDoublePlus,
        arr(42).toIntPlus,
        arr(43),
        arr(44).toDoublePlus,
        arr(45).toDoublePlus,
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
        arr(57).toIntPlus,
        arr(58).toDoublePlus,
        arr(59).toIntPlus,
        arr(60).toIntPlus,
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
        arr(73).toIntPlus,
        arr(74).toDoublePlus,
        arr(75).toDoublePlus,
        arr(76).toDoublePlus,
        arr(77).toDoublePlus,
        arr(78).toDoublePlus,
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        arr(84).toIntPlus)
    })
    //rdd[pojo]转DateFrame
    //导入隐式转换
    import spark.implicits._
    val df: DataFrame = res.toDF()
    println("df长度"+df.count())
    df.coalesce(1).write.parquet("f:/mrdata/dmpout1")
    spark.close()
  }
}
/*
//样例类封装数据信息
case class DMP(sessionid: String,
               advertisersid: Int,
               adorderid: Int,
               adcreativeid: Int,
               adplatformproviderid: Int,
               sdkversion: String,
               adplatformkey: String,
               putinmodeltype: Int,
               requestmode: Int,
               adprice: Double,
               adppprice: Double,
               requestdate: String,
               ip: String,
               appid: String,
               appname: String,
               uuid: String,
               device: String,
               client: Int,
               osversion: String,
               density: String,
               pw: Int,
               ph: Int,
               longtitute: String, //long  冲突 改为longtitute
               lat: String,
               provincename: String,
               cityname: String,
               ispid: Int,
               ispname: String,
               networkmannerid: Int,
               networkmannername: String,
               iseffective: Int,
               isbilling: Int,
               adspacetype: Int,
               adspacetypename: String,
               devicetype: Int,
               processnode: Int,
               apptype: Int,
               district: String,
               paymode: Int,
               isbid: Int,
               bidprice: Double,
               winprice: Double,
               iswin: Int,
               cur: String,
               rate: Double,
               cnywinprice: Double,
               imei: String,
               mac: String,
               idfa: String,
               openudid: String,
               androidid: String,
               rtbprovince: String,
               rtbcity: String,
               rtbdistrict: String,
               rtbstreet: String,
               storeurl: String,
               realip: String,
               isqualityapp: Int,
               bidfloor: Double,
               aw: Int,
               ah: Int,
               imeimd5: String,
               macmd5: String,
               idfamd5: String,
               openudidmd5: String,
               androididmd5: String,
               imeisha1: String,
               macsha1: String,
               idfasha1: String,
               openudidsha1: String,
               androididsha1: String,
               uuidunknow: String,
               userid: String,
               iptype: Int,
               initbidprice: Double,
               adpayment: Double,
               agentrate: Double,
               lrate: Double,
               adxrate: Double,
               title: String,
               keywords: String,
               tagid: String,
               callbackdate: String,
               channelid: String,
               mediatype: Int
              )*/
