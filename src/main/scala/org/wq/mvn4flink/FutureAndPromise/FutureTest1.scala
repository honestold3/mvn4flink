package org.wq.mvn4flink.FutureAndPromise

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Failure,Success,Try}

/**
  * Created by wq on 2016/10/23.
  */
object FutureTest1 extends App{

  val s ="Hello"

  val f:Future[String]= future {
    s +" future!"
  }

  f onComplete {

    case Success(t)=>{
      println(t)
    }
    case Failure(e)=>{
      println(s"An error has occured: $e.getMessage")
    }
  }

  println("dd")

}
