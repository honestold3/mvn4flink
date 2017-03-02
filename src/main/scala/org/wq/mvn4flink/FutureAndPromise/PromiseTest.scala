package org.wq.mvn4flink.FutureAndPromise

import java.util.Timer
import java.util.TimerTask

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
  * Created by wq on 2016/10/29.
  */
object PromiseTest {

  val timer =new Timer
  /** Return a Future which completes successfully with the supplied value after secs seconds. */
  def delayedSuccess[T](secs:Int, value: T):Future[T]={
    val result =Promise[T]
    timer.schedule(new TimerTask(){
      def run()={
        result.success(value)
      }
    }, secs *1000)
    result.future
  }
  /** Return a Future which completes failing with an IllegalArgumentException after secs
    * seconds. */
  def delayedFailure(secs:Int, msg:String):Future[Int]= {
    val result = Promise[Int]
    timer.schedule(new TimerTask() {
      def run() = {
        result.failure(new IllegalArgumentException(msg))
      }
    }, secs * 1000)
    result.future
  }

  def task1(input: Int) = PromiseTest.delayedSuccess(1, input + 1)
  def task2(input: Int) = PromiseTest.delayedSuccess(2, input + 2)
  def task3(input: Int) = PromiseTest.delayedSuccess(3, input + 3)
  def task4(input: Int) = PromiseTest.delayedSuccess(1, input + 4)


  def runBlocking() = {
    val v1 = Await.result(task1(1), Duration.Inf)
    val future2 = task2(v1)
    val future3 = task3(v1)
    val v2 = Await.result(future2, Duration.Inf)
    val v3 = Await.result(future3, Duration.Inf)
    val v4 = Await.result(task4(v2 + v3), Duration.Inf)
    val result = Promise[Int]
    result.success(v4)
    result.future
  }

  def runFlatMap() = {
    task1(1) flatMap {v1 =>
      val a = task2(v1)
      val b = task3(v1)
      a flatMap { v2 =>
        b flatMap { v3 => task4(v2 + v3) }}
    }
  }



  def main(args: Array[String]): Unit = {
    //runFlatMap
  }

}
