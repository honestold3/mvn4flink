package org.wq.mvn4flink.akka

import akka.actor.{ActorSystem, Props, Actor}

/**
  * Created by wq on 16/8/22.
  */


case class ExecutionMessage(msg: String)
case class StartMessage(msg: String)
case class FinishedMessage(msg: String)

class ChildActor extends Actor {
  def receive = {
    case ExecutionMessage(msg) =>
      println(msg) //fake real execution logic
      sender ! FinishedMessage("kill me!")
  }
}
class ParentActor extends Actor {
  val child = context.actorOf(Props[ChildActor], "ChildActor")

  def receive = {
    case StartMessage(msg) =>
      println(msg)
      child ! ExecutionMessage("executing child actor")
    case FinishedMessage(msg) =>
      println(msg)
  }
}

object TestApp extends App {
  val system = ActorSystem("testParentChildActors")
  val parentActor = system.actorOf(Props[ParentActor], "ParentActor")

  parentActor ! StartMessage("start")
  system.shutdown
}
