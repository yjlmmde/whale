package dmp.utils

import org.apache.spark.sql.Row

/**
  * Created by yjl
  * Data: 2019/3/1
  * Time: 17:08
  */
object FlagsUtil {
  def getFlags(tp: Row) = {
    val requestmode = tp.getAs[Int]("requestmode")
    val processnode = tp.getAs[Int]("processnode")
    val iseffective: Int = tp.getAs[Int]("iseffective")
    val isbilling: Int = tp.getAs[Int]("isbilling")
    val isbid: Int = tp.getAs[Int]("isbid")
    val iswin: Double = tp.getAs[Int]("iswin")
    val adorderid = tp.getAs[Int]("adorderid")
    val winprice = tp.getAs[Double]("winprice")
    val adpayment = tp.getAs[Double]("adpayment")
    var Array(orig_req, val_req, adv_req, bidding, bid_succ, adv_display, adv_cli, adv_consum, adv_costs) = Array(0, 0, 0, 0, 0, 0, 0, 0.0, 0.0)
    if (requestmode == 1 && processnode >= 1) orig_req = 1
    if (requestmode == 1 && processnode >= 2) val_req = 1
    if (requestmode == 1 && processnode == 3) adv_req = 1
    if (iseffective == 1 && isbilling == 1 && isbid == 1 && adorderid != 1) bidding = 1
    if (iseffective == 1 && isbilling == 1 && iswin == 1) bid_succ = 1
    if (requestmode == 2 && iseffective == 1) adv_display = 1
    if (requestmode == 3 && iseffective == 1) adv_cli = 1
    if (iseffective == 1 && isbilling == 1 && iswin == 1) {
      adv_consum = winprice * 1.0 / 1000
      adv_costs = adpayment * 1.0 / 1000
    }
    //标识字段(需要聚合的字段)
    val flag = List(orig_req, val_req, adv_req, bidding, bid_succ, adv_display, adv_cli, adv_consum, adv_costs)
    flag
  }

}
