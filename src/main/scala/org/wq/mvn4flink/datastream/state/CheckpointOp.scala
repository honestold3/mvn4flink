package org.wq.mvn4flink.datastream.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.{CheckpointedFunction, CheckpointedRestoring}
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
  * Created by wq on 2017/6/27.
  */
class CheckpointOp extends RichMapFunction[(String, Long),String] with CheckpointedFunction
  with CheckpointedRestoring[List[(String, Long)]] {

  private var state: ListState[CustomCaseClass] = _

  private val bufferedElements = new ListBuffer[(String, Long)]()

  override def open(config: Configuration): Unit = {

  }

  override def map(a: (String,Long)): String = {
    a._1
  }

  override def close(): Unit = {

  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    state = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[CustomCaseClass](
        "customState", createTypeInformation[CustomCaseClass]))
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    state.clear()
    state.add(CustomCaseClass("Here be", 123))
  }

  override def restoreState(state: List[(String, Long)]): Unit = {
    bufferedElements ++= state
  }

}
