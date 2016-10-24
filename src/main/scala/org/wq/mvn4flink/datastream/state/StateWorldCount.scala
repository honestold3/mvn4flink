package org.wq.mvn4flink.datastream.state

import org.apache.flink.streaming.api.scala._

import scala.collection.immutable.HashSet
import scala.collection.JavaConversions._
/**
  * Created by wq on 2016/10/21.
  */

case class Kankan

object StateWorldCount {

  def main(args: Array[String]): Unit = {
    //val env = StreamExecutionEnvironment.getExecutionEnvironment

    val env = StreamExecutionEnvironment.createLocalEnvironment()
    env.setParallelism(1)
    //env.addDefaultKryoSerializer(classOf[String], classOf[])

    val inputStream = env.fromElements("foo", "bar", "foobar", "bar", "barfoo", "foobar", "foo", "fo")


    // filter words out which we have already seen
    val uniqueWords = inputStream.keyBy(x => x).filterWithState{
      (word, seenWordsState: Option[Set[String]]) => seenWordsState match {
        case None => (true, Some(HashSet(word)))
        case Some(seenWords) => (!seenWords.contains(word), Some(seenWords + word))
      }
    }

    //uniqueWords.print()



    // count the number of incoming (first seen) words
    val numberUniqueWords = uniqueWords.keyBy(x => 0).mapWithState{
      (word, count: Option[Int]) => {
        val newCount = count.getOrElse(0) + 1
        val output = (word, newCount)
        (output, Some(newCount))
      }
    }

//    val numberUniqueWords = inputStream.keyBy(x => x).mapWithState{
//      (word, count: Option[Int]) => count match {
//          case None => (word, Some(1))
//          case Some(counter) => (word, Some(counter + 1))
//          //case _ => (word+"ss22", Some(100))
//        }
//    }
    //numberUniqueWords.print()


    println("-------------------------")



    val text = env.fromElements("foo", "bar", "foobar", "bar", "barfoo", "foobar", "foo", "fo")
    //val words = text.flatMap ( _.split(" ") )
    val kankan = text.keyBy(x => x).mapWithState {
      (word, count: Option[Int]) => {
        val newCount = count.getOrElse(0) + 1
        val output = (word, newCount)
        (output, Some(newCount))
      }
    }
    //kankan.print

    val stream: DataStream[(String,Int)] = env.fromElements(("foo",100),("foo",1),("foo",1),("foo",100))
    val counts: DataStream[(String, Int)] = stream
      .keyBy(_._1)
      .mapWithState((in: (String, Int), count: Option[Int]) =>
        count match {
          case None => ( (in._1, 1), Some(in._2) )
          case Some(c) => println(c);( (in._1, c + in._2), Some(c + in._2) )
        })

//    val counts: DataStream[(String, Int)] = stream
//          .keyBy(_._1)
//          .mapWithState((in: (String, Int), count: Option[Int]) =>
//            {
//              val newCount = count.getOrElse(0)
//              val output = (in._1,newCount+in._2)
//              (output,Some(newCount+in._2))
//            })

    counts.print()

    env.execute()
  }

}
