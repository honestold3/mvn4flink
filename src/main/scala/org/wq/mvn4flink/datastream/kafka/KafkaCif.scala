package org.wq.mvn4flink.datastream.kafka

import java.util.Properties
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.fs.{RollingSink, SequenceFileWriter, StringWriter, Writer}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

/**
  * Created by wq on 2017/6/20.
  */
object KafkaCif {

  private val ZOOKEEPER_HOST = "cift1.zookeeper.dc.puhuifinance.com:2181"
  private val KAFKA_BROKER = "cift1.kafka.dc.puhuifinance.com:6667"
  private val KAFKA_GROUP = "cif_output"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure Kafka consumer
    val kafkaProps = new Properties
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", KAFKA_GROUP)

    val kafkaConsumer = new FlinkKafkaConsumer010[String](
      "TrickletWjTest", KafkaStringSchema, kafkaProps)

    val dataStream = env.addSource(kafkaConsumer)

    val sink = new BucketingSink[String]("hdfs://cift1.flink.dc.puhuifinance.com:8020/data/cloud/flink")
    //sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
    sink.setWriter(new StringWriter[String]())
    sink.setBatchSize(1024 * 1024 *  200)
    sink.setPartPrefix("kkk")
    sink.setInProgressPrefix("is not ok")
    sink.setPendingPrefix("is ok")
    //sink.setInactiveBucketCheckInterval(2000)
    //sink.setPendingPrefix("bbbb")
    //sink.setPendingSuffix("AAAA")

    dataStream.map(_.toString).addSink(sink)


    env.execute("cif_kafka")
  }

}
