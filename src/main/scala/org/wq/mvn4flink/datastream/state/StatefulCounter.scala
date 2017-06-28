package org.wq.mvn4flink.datastream.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.checkpoint.Checkpointed

/**
  * Created by wq on 2017/6/27.
  */
class StatefulCounter extends RichMapFunction[String, (String,Int)] with Checkpointed[Integer]{

  private var count: Integer = 0

  override def map(in: String): (String,Int) = {
    count += 1
    (in, count)
  }

  override def snapshotState(l: Long, l1: Long): Integer = {
    count
  }

  override def restoreState(state: Integer) {
    count = state
  }

}
