package org.wq.mvn4flink.datastream.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * Created by wq on 16/9/26.
  */
object CountWindow {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.fromElements(1, 2, 3, 4)
    dataStream.countWindowAll(2).sum(0).print()
    env.execute("CountWindow demo")
  }
}
