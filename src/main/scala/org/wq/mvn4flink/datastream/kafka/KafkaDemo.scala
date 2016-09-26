package org.wq.mvn4flink.datastream.kafka

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer08, FlinkKafkaProducer08}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema


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
//    val stream = env
//      .addSource(new FlinkKafkaConsumer08[String]("input", new SimpleStringSchema(), kafkaProps))
//      .print

    val kafkaConsumer = new FlinkKafkaConsumer08[String](
      "input", KafkaStringSchema, kafkaProps)

    val kafkaproducer = new FlinkKafkaProducer08[String](
      "localhost:9092",
      "output",
      KafkaStringSchema
    )

    val dataStream = env.addSource(kafkaConsumer)

    dataStream.map(_.toString).addSink(kafkaproducer)


    env.execute("Flink Kafka Example")
  }

}
