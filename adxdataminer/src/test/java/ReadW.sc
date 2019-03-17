import scala.collection.mutable
import scala.io.Source

val stopwords = Source.fromFile("F:\\dmp/stopwords.txt","utf-8").getLines()
val stopwd_Map = new mutable.HashMap[String,Null]()
for (elem <- stopwords) {
  val arrs = elem.split(" ")
  for (i <- arrs){
    stopwd_Map.put(i,null)
  }
}
var ls = List[(String,Int)]()
var ls1 = List(("1",1),("2",1))
var ls2 = List(("3",1),("5",2))
ls ++ ls1
val ll = "123 123 ashd ald jj la b"
val fl = ll.split(" ").filter(_.size>2)
for (elem <- fl) {
  println(elem)
}
println(ls)