package org.wq.mvn4flink.datastream.socket

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{State, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.{Checkpointed, CheckpointedFunction}

import scala.collection.mutable

/**
  * Created by wq on 2017/6/21.
  */
class Rollop extends RichMapFunction[String, String] {

  var kkk: String = null

  val numLines = new IntCounter()

  private[this] val states = new mutable.HashMap[Int, State]()

  override def open(config: Configuration): Unit = {
    getRuntimeContext.addAccumulator("Counter", numLines)
    kkk = "hehe"
  }

  override def map(x: String): String = {
    numLines.add(1)
    x+kkk
  }

  override def close(): Unit = {

  }

}
