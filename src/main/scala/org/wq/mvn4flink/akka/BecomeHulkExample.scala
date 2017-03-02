package org.wq.mvn4flink.akka

import akka.actor.Actor.Receive
import akka.actor._

/**
  * Created by wq on 2016/10/9.
  *
  */

case object ActNormalMessage
case object TryToFindSolution
case object BadGuysMakeMeAngry

class DavidBanner extends Actor{
  import context._
  var a = 0

  def angryState: Receive = {
    case ActNormalMessage =>
      a = 1
      println("Phew, I'm back to being David")
      become(normalState)
      println(s"ending:$a")
    case "a" => println("haha")
  }

  def normalState: Receive = {
    case TryToFindSolution =>
      println("Looking for solution to my problem...")
    case BadGuysMakeMeAngry =>
      println("I'm getting angry...")
      become(angryState)
    case "a" => println("kankan")
  }

  def receive = {
    case BadGuysMakeMeAngry => become(angryState)
    case ActNormalMessage=> become(normalState)
    case "a" => println("qqq")
  }
}

object BecomeHulkExample extends App{

  val system = ActorSystem("BecomeHulkExample")
  val davidBanner = system.actorOf(Props[DavidBanner],name="DavidBanner")
  davidBanner ! "a"
  davidBanner ! ActNormalMessage //init to normalState

  davidBanner ! TryToFindSolution

  davidBanner ! BadGuysMakeMeAngry

  davidBanner ! "a"
  Thread.sleep(1000)
  davidBanner ! ActNormalMessage
  system.shutdown()
}
