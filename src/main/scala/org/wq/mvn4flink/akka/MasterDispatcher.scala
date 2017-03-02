package org.wq.mvn4flink.akka

import akka.actor.FSM.Failure
import akka.actor.Status.Success
import akka.actor._
import scala.concurrent.Future
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import scala.util.{Try}
import akka.actor.ActorSystem
import akka.actor.Status.Success
import akka.actor.Status.Failure
import akka.pattern.{ ask, pipe }
import scala.util.{Success => ScalaSuccess}
import scala.util.{Failure => ScalaFailure}
/**
  * Created by wq on 16/8/23.
  */

trait Message
case class InsertCommand(recordCount:Int) extends Message
case class ControlCommand(message: String, startTime: Long) extends Message
case class StartCommand(actorCount: Int) extends Message
case class ExecutionResult(costTime: Long) extends Message

class WriterActor extends Actor {
  override def preStart(): Unit = {
    println(Thread.currentThread().getName)
  }

  //def receive = ???

  def receive = {
    case _ =>{
      //
      while(true){
        println("kankan:"+Thread.currentThread().getName)
        println("----ddd")
      }

      //println("----ddd")
    }

  }
}

case class ExecutorResult(result : Try[Int])

class ControlActor extends Actor {
  implicit val timeout = Timeout(5 minutes)

  def receive = {
    case msg: StartCommand =>
      val startTime = System.currentTimeMillis()
      val actors = createActors(msg.actorCount)
//      val results = actors.map(actor => actor ? InsertCommand(100) mapTo manifest[Long])
//      val aggregate = Future.sequence(results).map(results => ExecutionResult(results.sum))
//
//      aggregate onComplete {
//        case ScalaSuccess(_) =>
//          val endTime = System.currentTimeMillis()
//          val costTime = endTime - startTime
//          println(s"It take total time ${costTime} milli seconds")
//        case ScalaFailure(_) => println("It failed.")
//      }
  }

  def createActors(count: Int): List[ActorRef] = {
    val props = Props(classOf[WriterActor]).withDispatcher("writer-dispatcher")
    (1 to count).map(i => {
      context.actorOf(props, s"writer_$i")! 1
    }).toList
    List(context.actorOf(props, s"writerWWW"))
  }
}

object MasterDispatcher {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("DataInitializer")
    val actor = system.actorOf(Props[ControlActor], "controller")

    actor ! StartCommand(10000)

    system.shutdown()
  }
}
