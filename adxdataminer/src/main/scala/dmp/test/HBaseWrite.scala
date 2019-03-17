package dmp.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yjl
  * Data: 2019/3/6
  * Time: 14:19
  */
object HBaseWrite {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWriteHBase").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val tablename = "t1"
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE,"t1")
    conf.set("hbase.zookeeper.quorum","lu2:2181,lu2:2181,lu2:2181")

    val hdpconf = new Configuration(conf)
    val job = Job.getInstance(hdpconf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    // 构造两条测试数据
    val indataRDD = sc.makeRDD(Array("323,APP_DDZ,3","434,APP_MM,27")) //构建两行记录

    // 将测试数据rdd 转换成  kv元组 RDD ，k为将写入hbase的行键，v为数据封装put对象，封装一行数据
    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0))) //行健的值
      put.addColumn(Bytes.toBytes("p"),Bytes.toBytes(arr(1)),Bytes.toBytes(arr(2)))
      (new ImmutableBytesWritable, put)
    }}


    // 将rdd写入hbase
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)

  }

}
