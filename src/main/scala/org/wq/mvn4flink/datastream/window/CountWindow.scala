package org.wq.mvn4flink.datastream.window

import java.util.Properties
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.wq.mvn4flink.datastream.kafka.KafkaStringSchema


/**
  * Created by wq on 16/9/26.
  */
object CountWindow {

  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER = "localhost:9092"
  private val KAFKA_GROUP = "input"

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements(1, 2, 3, 4)

    val kankan = data.countWindowAll(2).sum(0).print()

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // configure Kafka consumer
    val kafkaProps = new Properties
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", KAFKA_GROUP)
    //    val stream = env
    //      .addSource(new FlinkKafkaConsumer08[String]("input", new SimpleStringSchema(), kafkaProps))
    //      .print

    val kafkaConsumer = new FlinkKafkaConsumer010[String](
      "input", KafkaStringSchema, kafkaProps)

    val dataStream = env.addSource(kafkaConsumer)

    val tumbling = dataStream.keyBy(0).countWindow(100).sum(0)

    val sliding = dataStream.keyBy(0).countWindow(100,10).sum(0)


    env.execute("CountWindow demo")
  }
}
