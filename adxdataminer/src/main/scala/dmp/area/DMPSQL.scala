package dmp.area

import java.util.Properties

import com.typesafe.config.ConfigFactory
import dmp.clean.DMP
import dmp.config.DMPConfig
import dmp.utils.{FlagsUtil, SessionUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * Created by yjl
  * Data: 2019/2/27
  * Time: 0:16
  */
object DMLSQL {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //伪装用户身份
  System.setProperty("HADOOP_USER_NAME", "root")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SessionUtil.getSpark
    //导入隐式转换
    import spark.implicits._
    val df: DataFrame = spark.read.parquet("f:/mrdata/dmpout")
    val ds = df.map(tp => {
      val provincename: String = tp.getAs[String]("provincename")
      val cityname: String = tp.getAs[String]("cityname")
      (provincename, cityname)
    })
    //2,省市数据分布概况报表
    //方法1 ,截取相应字段
    /*  val df2: DataFrame = ds.toDF("provincename","cityname")
      df2.createTempView("province_city")*/
    //方法2 ,直接操作表结构数据,截取时间字段
    df.createTempView("province_city")
    val last: DataFrame = spark.sql("select substr(requestdate,0,10) day, provincename ,cityname ,count(1) from province_city group by day ,provincename,cityname")
    //    last.show(10)
    //    last.write.jdbc(url,"pro_day_sql",prop)
    //3 地域维度KPI报表
    //DSL风格
    import org.apache.spark.sql.functions._
    //col("字段")==$"字段" = '字段
    //requestmode	processnode	iseffective	isbilling	isbid	iswin	adorderid
    //分组函数
    val adv_prodf_dsl: DataFrame = df.groupBy(substring($"requestdate", 0, 10).as("day"), 'provincename.as("pro"), 'cityname.as("city"))
      //聚合函数
      .agg(
      sum(when('requestmode === 1 and ('processnode >= 1), 1).otherwise(0)).as("orig_req"), //原始请求
      sum(when('requestmode === 1 and ('processnode >= 2), 1).otherwise(0)).as("val_req"), //有效请求
      sum(when('requestmode === 1 and ('processnode === 3), 1).otherwise(0)).as("adv_req"), //广告请求
      sum(when('iseffective === 1 and ('isbilling === 1) and ('isbid === 1) and ('adorderid =!= 0), 1).otherwise(0)).as("bidding"), //参与竞价
      sum(when('iseffective === 1 and ('isbilling === 1) and ('iswin === 1), 1).otherwise(0)).as("bid_succ"), //竞价成功
      sum(when('requestmode === 2 and ('iseffective === 1), 1).otherwise(0)).as("adv_display"), //广告展示
      sum(when('requestmode === 3 and ('iseffective === 1), 1).otherwise(0)).as("adv_cli"), //广告点击
      sum(when('iseffective === 1 and ('isbilling === 1) and ('iswin === 1), 'winprice / 1000d).otherwise(0.0)).as("adv_consum"), //广告消费
      sum(when('iseffective === 1 and ('isbilling === 1) and ('iswin === 1), 'adpayment / 1000d).otherwise(0.0)).as("adv_costs") //广告成本
    )
    //spark_sql
    val adv_prodf_sql: DataFrame = spark.sql(
      """
        select substr(requestdate,0,10) as day,
        provincename province,
        cityname city,
        sum(case when requestmode == 1 and processnode >= 1 then 1 else 0 end) as orig_req,
        sum(case when requestmode == 1 and processnode >= 2 then 1 else 0 end) as val_req,
        sum(case when requestmode == 1 and processnode == 3 then 1 else 0 end) as adv_req,
        sum(if(iseffective==1 and isbilling == 1 and isbid == 1 and adorderid != 1 ,1 ,0)) as bidding,
        sum(if(iseffective==1 and isbilling == 1 and iswin==1 ,1 ,0)) as bid_succ,
        sum(if(requestmode == 2 and iseffective == 1 ,1 ,0)) as adv_display,
        sum(if(requestmode == 3 and iseffective == 1 ,1 ,0)) as adv_cli,
        sum(if(iseffective == 1 and isbilling == 1 and iswin == 1 , winprice * 1.0/1000 ,0.0)) as adv_consum,
        sum(if(iseffective == 1 and isbilling == 1 and iswin == 1 , adpayment * 1.0/1000 ,0.0)) as adv_costs
        from province_city
        group by day,province,city
      """.stripMargin)
    //spark_rdd做法
    val data_rdd: RDD[((String, String, String), List[Double])] = df.rdd.map(f = tp => {
      //根据字段类型,通过字段名取值
      //天
      val day: String = tp.getAs[String]("requestdate").substring(0, 10)
      //省份
      val provincename: String = tp.getAs[String]("provincename")
      //城市
      val cityname: String = tp.getAs[String]("cityname")


      //使用封装好的工具类获取flag标识指标
      val flag: List[Double] = FlagsUtil.getFlags(tp)
      ((day, provincename, cityname), flag)
    })

    //分组聚合
    val pro_rdd: RDD[((String, String, String), List[Double])] = data_rdd.reduceByKey((lst1, lst2) => {
      lst1.zip(lst2).map(tp => tp._1 + tp._2)
    })
    //dataset ==> dataframe
    val adv_prodf_rdd: DataFrame = pro_rdd.map(tp => {
      //把数据使用样例类封装
      ProSqlBean(tp._1._1, tp._1._2, tp._1._3, tp._2(0).toInt, tp._2(1).toInt, tp._2(2).toInt, tp._2(3).toInt, tp._2(4).toInt, tp._2(5).toInt, tp._2(6).toInt, tp._2(7), tp._2(8))
    }).toDF()

    import scala.collection.JavaConversions._

    adv_prodf_dsl.show(10)

//    adv_prodf_dsl.write.mode(SaveMode.Overwrite).jdbc(DMPConfig.url, "adv_pro_dsl", DMPConfig.prop)
//    adv_prodf_rdd.write.mode(SaveMode.Overwrite).jdbc(url,"adv_prodf_rdd",prop)
    spark.close()

  }
}
//样例类封装schema信息
case class ProSqlBean(day: String, provincename: String, cityname: String, orig_req:Int, val_req: Int, adv_req: Int, bidding: Int, bid_succ: Int, adv_display: Int, adv_cli: Int, adv_consum:Double, adv_costs: Double)
