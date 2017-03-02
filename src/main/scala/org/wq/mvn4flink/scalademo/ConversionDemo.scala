package org.wq.mvn4flink.scalademo

/**
 * Created by wq on 14-7-28.
 */
object ConversionDemo {

  def main(args: Array[String]) {

    val javamap: java.util.Map[String,String] = new java.util.HashMap[String,String]
    javamap.put("Hi","there")
    javamap.put("So","long")

    import scala.collection.JavaConversions._
    javamap.foreach(kv => println(kv._1 + " ->" + kv._2))

  }
}
