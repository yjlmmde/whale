package dmp.utils

import org.apache.spark.sql.SparkSession

/**
  * Created by yjl
  * Data: 2019/2/26
  * Time: 23:21
  */
object SessionUtil {
  def getSpark={
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DMLDemo")
      .enableHiveSupport()
      .getOrCreate()

    spark
  }
}
