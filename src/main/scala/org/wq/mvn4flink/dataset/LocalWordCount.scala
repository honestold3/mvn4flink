package org.wq.mvn4flink.dataset

import org.apache.flink.api.scala._

/**
  * Created by wq on 16/9/13.
  */
object LocalWordCount {

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.createLocalEnvironment()

    val word = env.fromElements("hello world", "hi hello", "hello world", "hi hello", "hi hello")
    val kk = word.flatMap{x => val ss = x.split(" "); ss.map((_,1))}.groupBy(0).sum(1)

    kk.print()

    println("--------------------------------------------------------------")

    val text = env.readTextFile("hdfs://honest:8020/wq/wc.txt")
    val count = text.flatMap(_.split(" ")).map{(_,1)}.groupBy(0).sum(1)

    count.print()

    //env.execute("LocalWordCount")
  }

}
