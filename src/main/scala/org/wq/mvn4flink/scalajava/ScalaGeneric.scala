package org.wq.mvn4flink.scalajava

import java.util.List
import scala.collection.JavaConversions._

/**
  * Created by wq on 2016/10/11.
  */
class ScalaGeneric {

  def printList(list: List[T] forSome {type T}):Unit={
    list.foreach(println)
  }

  def printList2(list: List[_]):Unit={
    list.foreach(println)
  }

}

object MainGeneric extends App{
  val s = new ScalaGeneric
  s.printList(JavaGeneric.getList)
  println("-----------------------------")
  s.printList2(JavaGeneric.getList)
}
