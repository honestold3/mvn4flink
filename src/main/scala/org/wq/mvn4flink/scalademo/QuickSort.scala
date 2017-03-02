package org.wq.mvn4flink.scalademo

import scala.collection.mutable.ListBuffer

/**
  * Created by wq on 15/12/13.
  */
object QuickSort extends App{

  def sort(xs: Array[Int]): Array[Int]= {
    if(xs.length<=1){
      xs
    }else{
      val pivot = xs(xs.length/2)

      val a1 = new ListBuffer[Int]()
      val a2 = new ListBuffer[Int]()
      val a3 = new ListBuffer[Int]()

      xs.foreach{
        case x if x < pivot => a1+=x
        case y if y == pivot => a2+=y
        case z if z > pivot => a3+=z
        case _ => null
      }

      Array.concat(sort(a1.toArray),a2.toArray,sort(a3.toArray))
      //Array.concat(sort(xs.filter(pivot >)),xs.filter(pivot ==),sort(xs.filter(pivot <)))
    }


  }

  val arr = Array(4,6,9,2,8,3)
  //sort(arr).map(println _)

  sort(Array(4,5,3,1,8,6,0)).map(println _)

}
