package org.wq.mvn4flink.outputformat

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by wq on 2017/6/26.
  */
class MultipleTextOutputFormatSinkFunction[IN](descPath: String) extends RichSinkFunction[IN] {
  val map = mutable.Map[String, TextOutputFormat[String]]()
  var cleanupCalled = false
  val LOG = LoggerFactory.getLogger(classOf[MultipleTextOutputFormatSinkFunction[_]])
  var parameters: Configuration = null;

  override def open(parameters: Configuration) {
    this.parameters = parameters
  }

  override def invoke(item: IN): Unit = {
    val tuple = item.asInstanceOf[(String, String)]
    val key = tuple._1
    val value = tuple._2
    val result = map.get(key)
    val format = if (result.isDefined) {
      result.get
    } else {
      val textOutputFormat = new TextOutputFormat[String](new Path(descPath, key))
      textOutputFormat.configure(parameters)
      val context: RuntimeContext = getRuntimeContext
      val indexInSubtaskGroup: Int = context.getIndexOfThisSubtask
      val currentNumberOfSubtasks: Int = context.getNumberOfParallelSubtasks
      textOutputFormat.open(indexInSubtaskGroup, currentNumberOfSubtasks)
      map.put(key, textOutputFormat)
      textOutputFormat
    }
    try {
      format.writeRecord(value)
    }
    catch {
      case ex: Exception => {
        cleanup()
        throw ex
      }
    }

  }

  override def close() {
    try {
      map.foreach(_._2.close())
    } catch {
      case ex: Exception => {
        cleanup()
        throw ex
      }
    } finally {
      map.clear()
    }
  }

  private def cleanup() {
    try {
      if (!cleanupCalled) {
        cleanupCalled = true
        map.foreach(item => item._2.asInstanceOf[CleanupWhenUnsuccessful].tryCleanupOnError())
      }
    }
    catch {
      case t: Throwable => {
        LOG.error("Cleanup on error failed.", t)
      }
    }
  }
}
