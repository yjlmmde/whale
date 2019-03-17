package dmp.utils

/**
  * Created by yjl
  * Data: 2019/3/4
  * Time: 16:30
  */
class RichString(s: String) {
  def toIntPlus={
    try {
      s.toInt
    } catch {
      case e:Exception => 0
    }
  }
  def toDoublePlus ={
    try {
      s.toDouble
    } catch {
      case e:Exception => 0d
    }
  }

}
object RichString{
  def apply(s: String): RichString = new RichString(s)
  implicit def str2RichString(s:String):RichString={
     RichString(s)
  }
}