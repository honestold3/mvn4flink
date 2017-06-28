package org.wq.mvn4flink.datastream.elasticsearch

import java.util
import java.util.Properties
import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, RichSinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, FlinkKafkaConsumer010}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.transport.{TransportAddress, InetSocketTransportAddress}
import org.wq.mvn4flink.datastream.kafka.KafkaStringSchema
import org.apache.flink.streaming.api.datastream._

import scala.collection
import scala.collection.JavaConverters._
import org.apache.flink.util.Collector


/**
  * Created by wq on 16/9/20.
  *
  *
PUT http://honest:9200/flink-test
{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    }
  }
}

PUT /flink-test/_mapping/fink_test_data
{
    "properties": {
        "data": {
            "type": "string",
            "index": "not_analyzed"
        },
        "log": {
            "type": "string"
        }
    }
}
  */
object ElasticSearchDemo {
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


    val kafkaConsumer = new FlinkKafkaConsumer010[String](
      "input", KafkaStringSchema, kafkaProps)

    val kafkaproducer = new FlinkKafkaProducer010[String](
      "localhost:9092",
      "output",
      KafkaStringSchema
    )

    val ds = env.addSource(kafkaConsumer)

    //ds.addSink(kafkaproducer)

    // configure Elasticsearch
    val config: java.util.Map[String,String] = new java.util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", "elasticsearch")

    val map = Map("bulk.flush.max.actions"->"1","cluster.name"->"my-cluster-name")

    val transports  = new java.util.ArrayList[InetSocketTransportAddress]()
    transports.add(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
    val list = transports.asInstanceOf[java.util.List[TransportAddress]]

    val indexRequestBuilder = new IndexRequestBuilder[String]{
      override def createIndexRequest(element: String, ctx: RuntimeContext): IndexRequest = {
        val json = new java.util.HashMap[String, String]
        if(element.contains(",")){
          val k = element.split(",")
          json.put("log",k(1))
          json.put("data", k(0))
        }
        println("SENDING: " + element)
        Requests.indexRequest.index("flink-test").`type`("fink_test_data").source(json)
      }
    }

    val essink = new ElasticsearchSink(config, list, indexRequestBuilder) with SinkFunction[String]{
    }

    ds.addSink(essink)



    ds.addSink(new RichSinkFunction[String]{
      val received = new util.HashSet[String]()
      override def open(parameters: Configuration) {

      }
      override def invoke(in: String) = { println(s"in:$in");received.add(in) }
      override def close() = {
        assert(received.size() == 4)
        assert(received.contains("First"))
        assert(received.contains("Second"))
        assert(received.contains("FirstHello"))
        assert(received.contains("Firstworld"))
      }
    })

//    lazy val kk: Eskanan = new Eskanan()
//        ds.addSink(kk)

    class Eskanan extends RichSinkFunction[String] with SinkFunction[String]{
      val received = new util.HashSet[String]()
      override def open(parameters: Configuration) {

      }
      override def invoke(in: String) = { received.add(in) }
      override def close() = {
        assert(received.size() == 4)
        assert(received.contains("First"))
        assert(received.contains("Second"))
        assert(received.contains("FirstHello"))
        assert(received.contains("Firstworld"))
      }
    }

//
//    lazy val es = new EsSink("honest",9300)
//        ds.addSink(es)
//
//    //val data = ds.flatMap(_.split(","): Array[String]).map(x=>(x(0),x(1)))
//    //data.addSink(es)
//
//
//    class EsSink(host: String, port: Int)
//      extends WqElasticsearchSink[String](
//        host,
//        port,
//        "elasticsearch",
//        "flink-test",
//        "fink_test_data") with SinkFunction[String]{
//
//      override def insertJson(r: String): Map[String, String] = {
//        val k = r.split(",")
//        Map(
//          "data" -> k(0),
//          "log" -> k(1)
//        )
//      }
//
//      override def updateJson(r: String): Map[String, String] = {
//        val k = r.split(",")
//        Map[String, String] (
//          "log" -> k(1)
//        )
//      }
//
//      override def indexKey(r: String): String = {
//        val k = r.split(",")
//        // index by location and time
//        k(0) + "/" + k(1)
//      }
//    }

    env.execute("Flink ElasticSearch Example")
  }


}