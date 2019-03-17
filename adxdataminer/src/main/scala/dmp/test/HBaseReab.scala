package dmp.test

import dmp.utils.SessionUtil
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable


/**
  * Created by yjl
  * Data: 2019/3/6
  * Time: 14:19
  */
object HBaseReab {
  def main(args: Array[String]): Unit = {
    val spark = SessionUtil.getSpark

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE,"t1")
    conf.set("hbase.zookeeper.quorum","lu2:2181,lu2:2181,lu2:2181")

    // 将hbase中的表映射成rdd，rdd中为kv元组，k为hbase表中的rowkey， v为hbase表中一行数据的封装对象Result
    val hbaserdd = spark.sparkContext.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])

    val resRDD = hbaserdd.map(tp=>{
      val tagMap = new mutable.HashMap[String,String]()
      val uid = Bytes.toString(tp._1.copyBytes())
      while(tp._2.advance()){
        val cell = tp._2.current()
        val kBytes = CellUtil.cloneQualifier(cell)
        val vBytes = CellUtil.cloneValue(cell)
        val key = Bytes.toString(kBytes)
        val value = Bytes.toString(vBytes)
        tagMap += key -> value
      }
      (uid,tagMap)
    })

    resRDD.take(5).foreach(println)

    spark.close()
  }

}
