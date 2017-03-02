package org.wq.mvn4flink.scalajava

import java.util.ArrayList;
import scala.collection.JavaConversions._

/**
  * Created by wq on 2016/10/11.
  */
object InvokeJavaCollections extends App{

  def getList={
    val list=new ArrayList[String]()
    list.add("1111")
    list.add("2222")
    list
  }

  val list=getList
  list.foreach(println)
  val list2=list.map(x=>x*2)
  println(list2)

  //显式地进行转换
  val listStr=asJavaIterable(list)
  for(i<- listStr)
    println(i)


}
