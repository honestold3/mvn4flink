package org.wq.mvn4flink.dataset

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.hadoop.mapred.HadoopOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.wq.mvn4flink.outputformat.MyMultipleTextOutputFormat

/**
  * Created by wq on 2017/6/26.
  */
object FlinkMutipleFile {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val multipleTextOutputFormat = new MyMultipleTextOutputFormat[String, String]()
    val jc = new JobConf()
    FileOutputFormat.setOutputPath(jc, new Path("hdfs:///user/iteblog/"))
    val format = new HadoopOutputFormat[String, String](multipleTextOutputFormat, jc)
    val batch = env.fromCollection(List(("A", "1"), ("A", "2"), ("A", "3"),
      ("B", "1"), ("B", "2"), ("C", "1"), ("D", "2")))
    batch.output(format)
    env.execute("MultipleTextOutputFormat")
  }

}
