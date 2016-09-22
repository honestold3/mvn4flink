package org.wq.mvn4flink.dataset

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConversions._
import org.apache.flink.api.common.functions._

/**
  * Created by wq on 16/9/13.
  */
object WordCount {

  def main(args: Array[String]) {
    val env =  ExecutionEnvironment.getExecutionEnvironment
//benv.readTextFile("hdfs://honest:8020/wq/wc.txt").flatMap(_.split(" ")).map{(_,1)}.groupBy(0).sum(1).print
//    val text = env.readTextFile("hdfs://honest:8020/wq/wc.txt")
//    val count = text.flatMap(_.split(" ")).map{(_,1)}.groupBy(0).sum(1)
//
//    count.print()

    //env.execute("WordCount")

    val toBroadcast = env.fromElements(1, 2, 3)

    val data = env.fromElements("a", "b")

    //val bc = data.withBroadcastSet(data,"kankan")


    data.map(new RichMapFunction[String, String]() {
      var broadcastSet: Traversable[String] = null

      override def open(config: Configuration): Unit = {
        // 3. Access the broadcasted DataSet as a Collection
        broadcastSet = getRuntimeContext().getBroadcastVariable[String]("broadcastSetName")

        //println(s"$broadcastSet")
      }

      def map(in: String): String = {
        println(s"$broadcastSet")
        in
      }
    }).withBroadcastSet(toBroadcast, "broadcastSetName").print // 2. Broadcast the DataSet
  }

}
