package org.wq.mvn4flink.datastream.window

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer08, FlinkKafkaProducer08}
import org.wq.mvn4flink.datastream.kafka.KafkaStringSchema
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by wq on 2016/10/21.
  */
object TimeWindow {

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

    //dataStream.map(_.toString).addSink(kafkaproducer)

    val tumbling = dataStream.keyBy(0).timeWindow(Time.seconds(30)).sum(0)

    val sliding = dataStream.keyBy(0).timeWindow(Time.seconds(30),Time.seconds(10)).sum(0)

    val session = dataStream.keyBy(0).window(ProcessingTimeSessionWindows.withGap(Time.seconds(30))).sum(1)


    env.execute("Flink Kafka Example")
  }

}
