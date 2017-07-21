package org.wq.mvn4flink.datastream.socket

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Properties

import org.apache.flink.api.java.tuple.{Tuple, Tuple2}
import org.apache.flink.api.scala.hadoop.mapred.HadoopOutputFormat
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs._
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.wq.mvn4flink.datastream.kafka.KafkaStringSchema
import org.wq.mvn4flink.datastream.state.CheckpointOp
import org.wq.mvn4flink.outputformat.{MultipleTextOutputFormatSinkFunction, MyMultipleTextOutputFormat}
import org.wq.sink.Mysink

/**
 * This example shows an implementation of WordCount with data from a text socket. 
 * To run the example make sure that the service providing the text data is already up and running.
 *
 * To start an example socket text stream on your local machine run netcat from a command line, 
 * where the parameter specifies the port number:
 *
 * {{{
 *   nc -lk 9999
 * }}}
 *
 * Usage:
 * {{{
 *   SocketTextStreamWordCount <hostname> <port> <output path>
 * }}}
 *
 * This example shows how to:
 *
 *   - use StreamExecutionEnvironment.socketTextStream
 *   - write a simple Flink Streaming program in scala.
 *   - write and use user-defined functions.
 */
object SocketTextStreamWordCount {

  private val ZOOKEEPER_HOST = "localhost:2181"
  private val BOOTSTRAP_SERVER = "localhost:9092"
  //private val KAFKA_BROKER = "localhost:9092"
  private val KAFKA_GROUP = "input"

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>")
      return
    }

    val hostName = args(0)
    val port = args(1).toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // start a checkpoint every 1000 ms
    env.enableCheckpointing(1000)
    // advanced options:

    // set mode to exactly-once (this is the default)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // checkpoints have to complete within one minute, or are discarded
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    // allow only one checkpoint to be in progress at the same time
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)



    //env.setStateBackend(new MemoryStateBackend(5 * 1024 * 1024))
    env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoints"))
    //env.setStateBackend(new RocksDBStateBackend("file:///flink/checkpoints"))

    //env.setParallelism(4)
    //env.setMaxParallelism(4)

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream(hostName, port)

    val data = text.map{ x =>x }

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map{ x =>(x, 1) }
      .keyBy(0)
      .sum(1)
    counts.print

    val kafkaProps = new Properties
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVER)
    //kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", KAFKA_GROUP)
    kafkaProps.setProperty("auto.offset.reset", "earliest") // read from the beginning. (earliest is kafka 0.10 value)
    kafkaProps.setProperty("max.partition.fetch.bytes", "256") // make a lot of fetches (MESSAGES MUST BE SMALLER!)

    val kafkaproducer = new FlinkKafkaProducer010[String](
      "localhost:9092",
      "output",
      KafkaStringSchema
    )


    val sink = new BucketingSink[String]("hdfs:///base/path")
    //sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd"))
    sink.setWriter(new StringWriter[String]())
    sink.setBatchSize( 1024 * 1024 * 100)
    sink.setPartPrefix("kkk")
    sink.setInactiveBucketCheckInterval(2 * 60 * 1000)
    sink.setInactiveBucketThreshold(2 * 60 * 1000)
    //sink.setPendingPrefix("bbbb")
    //sink.setPendingSuffix("AAAA")

    //val mysink = new Mysink[org.apache.flink.api.java.tuple.Tuple2[String,String]]("hdfs:///mysink")
    //mysink.setWriter(new StringWriter[org.apache.flink.api.java.tuple.Tuple2[String,String]]())
    val mysink = new Mysink[(String,String)]("hdfs:///mysink")
    mysink.setWriter(new StringWriter[(String,String)]())
    mysink.setBatchSize(1024 * 1024 *  100)

    val ds = data.map{x =>
      val ss= x.toString.split(",")
      //new org.apache.flink.api.java.tuple.Tuple2[String,String](ss(0),ss(1))
      (ss(0),ss(1))
    }
    //ds.addSink(new MultipleTextOutputFormatSinkFunction[(String, String)]("hdfs:///outputs/"))

    val multipleTextOutputFormat = new MyMultipleTextOutputFormat[String, String]()
    val jc = new JobConf()
    FileOutputFormat.setOutputPath(jc, new Path("hdfs:///multiples/"))
    val format = new HadoopOutputFormat[String, String](multipleTextOutputFormat, jc)
    //ds.writeUsingOutputFormat(format)


    data.map(_.toString).addSink(sink)

    //data.map(new Rollop).addSink(sink)

    //ds.addSink(mysink)

    //data.map(_.toString).addSink(kafkaproducer)

    //ds.map{x=>(x._1,1L)}.map(new CheckpointOp)

    val rs = env.execute("Scala SocketTextStreamWordCount Example")
    val counter: Int = rs.getAccumulatorResult("Counter")
    println("counter : " + counter)
  }

}
