package org.wq.mvn4flink.akka

import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorSystem
import akka.routing.RoundRobinPool

/**
  * Created by wq on 16/8/18.
  */

// 定义一个case类
sealed trait SumTrait
case class Result(value: Int) extends SumTrait

// 计算用的Actor
class SumActor extends Actor {
  val RANGE = 10000

  def calculate(start: Int, end: Int, flag : String): Int = {
    var cal = 0

    for (i <- (start to end)) {
      //for (j <- 1 to 3000000) {}
      cal += i
    }

    println("flag:" + flag + ":"+cal)
    println("kk:"+self.path)
    return cal
  }

  def receive = {
    case value: Int =>
      println(sender.path)
      //sender ! Result(calculate((RANGE / 4) * (value - 1) + 1, (RANGE / 4) * value, value.toString))
      context.actorSelection("/user/masterActor") ! Result(calculate((RANGE / 4) * (value - 1) + 1, (RANGE / 4) * value, value.toString))
    case _ => println("未知 in SumActor")
  }
}

// 打印结果用的Actor
class PrintActor extends Actor {
  println("kkk:"+self.path)
  def receive = {
    case (sum: Int, startTime: Long) =>
      println("total:" + sum + "time is:"
        + (System.nanoTime() - startTime)/1000000000.0 + "s")
    case _ => println("未知 in PrintActor")
  }
}

// 主actor，发送计算指令给SumActor，发送打印指令给PrintActor
class MasterActor extends Actor {
  var sum = 0
  var count = 0
  var startTime: Long = 0

  // 声明Actor实例，nrOfInstances是pool里所启routee（SumActor）的数量，
  // 这里用4个SumActor来同时计算，很Powerful
  val sumActor   = context.actorOf(Props[SumActor]
    .withRouter(RoundRobinPool(nrOfInstances = 4)), name = "sumActor")
  val printActor = context.actorOf(Props[PrintActor], name = "printActor")

  def receive = {
    case "calculate..." =>
      startTime = System.nanoTime()
      for (i <- 1 to 4) sumActor ! i
    case Result(value) =>
      println("kankan")
      sum += value
      count += 1
      if (count == 4) {
        printActor ! (sum, startTime)
        context.stop(self)
      }
    case _ => println("未知 in MasterActor...")
  }
}

object Sum {
  def main(args: Array[String]): Unit = {
    var sum = 0

    val system = ActorSystem("MasterActorSystem")
    val masterActor = system.actorOf(Props[MasterActor], name = "masterActor")
    masterActor ! "calculate..."

    Thread.sleep(5000)
    system.shutdown()
  }
}
