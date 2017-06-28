package org.wq.mvn4flink.outputformat

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
  * Created by wq on 2017/6/26.
  */
class MyMultipleTextOutputFormat [K, V] extends MultipleTextOutputFormat[K, V]{

  override def generateActualKey(key: K, value: V): K =
    NullWritable.get().asInstanceOf[K]

  override def generateFileNameForKeyValue(key: K, value: V, name: String): String =
    key.asInstanceOf[String]

}
