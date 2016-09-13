package org.wq.mvn4flink.dataset

import org.apache.flink.api.scala._

/**
  * Created by wq on 16/9/13.
  */
object WordCount {

  def main(args: Array[String]) {
    val env =  ExecutionEnvironment.getExecutionEnvironment
//benv.readTextFile("hdfs://honest:8020/wq/wc.txt").flatMap(_.split(" ")).map{(_,1)}.groupBy(0).sum(1).print
    val text = env.readTextFile("hdfs://honest:8020/wq/wc.txt")
    val count = text.flatMap(_.split(" ")).map{(_,1)}.groupBy(0).sum(1)

    count.print()

    env.execute("WordCount")
  }

}
