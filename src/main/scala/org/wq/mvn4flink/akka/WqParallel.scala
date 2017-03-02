package org.wq.mvn4flink.akka

/**
 * Created by wq on 14-3-19.
 */
object WqParallel extends App{

  val urls = List("http://www.baidu.com","http://www.baidu.com")

  def gethtml(url : String) = scala.io.Source.fromURL(url).getLines().mkString("\n")

  val t = System.currentTimeMillis()

  //urls.map(gethtml(_))
  urls.par.map(gethtml(_))

  println("total time: "+(System.currentTimeMillis() - t) + "ms")
}
