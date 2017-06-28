package org.wq.mvn4flink.datastream.kafka

import java.util.Properties

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.fs.{RollingSink, SequenceFileWriter, StringWriter, Writer}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.hadoop.io.{BytesWritable, NullWritable}

import scala.collection.JavaConverters.asScalaIteratorConverter


/**
  * Created by wq on 16/9/19.
  */
object KafkaDemo {

  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER = "localhost:9092"
  private val KAFKA_GROUP = "input"

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // configure Kafka consumer
    val kafkaProps = new Properties
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", KAFKA_GROUP)
    kafkaProps.setProperty("auto.offset.reset","smallest")
//    val stream = env
//      .addSource(new FlinkKafkaConsumer08[String]("input", new SimpleStringSchema(), kafkaProps))
//      .print

    val kafkaConsumer = new FlinkKafkaConsumer010[String](
      "input", KafkaStringSchema, kafkaProps)

    val kafkaproducer = new FlinkKafkaProducer010[String](
      "localhost:9092",
      "output",
      KafkaStringSchema
    )

    val dataStream = env.addSource(kafkaConsumer)

    val sink = new BucketingSink[String]("/base/path")
    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
    sink.setWriter(new StringWriter[String]())
    sink.setBatchSize(1024 * 1024 * 400)


    dataStream.map(_.toString).addSink(kafkaproducer)

    dataStream.map(_.toString).addSink(sink)

    env.execute("Flink Kafka Example")
  }

}
