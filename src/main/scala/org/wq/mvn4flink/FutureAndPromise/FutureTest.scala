package org.wq.mvn4flink.FutureAndPromise

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

/**
  * Created by wq on 2016/10/23.
  */
object FutureTest extends App{

  val s ="Hello"

  val f:Future[String]= future {
    Thread.sleep(2000)
    println("hhhhhh")
    s +" future!"
  }
//  f onSuccess {
//    case msg => println(msg)
//  }
//  f onFailure {
//    case msg => println(msg)
//  }

  f onComplete {
    case Success(msg) => println("jjjj:"+msg)
    case Failure(msg) => println(msg)
  }
  println(s)//不加这句, f onSuccess就不执行

  Thread.sleep(5000)
  println("finished")
}
