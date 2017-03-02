package org.wq.mvn4flink.mytype

import org.apache.hadoop.mapred.InputFormat

import scala.reflect.ClassTag

/**
  * Created by wq on 2016/10/15.
  */
class MyType[T]

object TestClassTag {
  def arrayMake[T: ClassTag](first: T, second: T) = {
    val r = new Array[T](2)
    r(0) = first
    r(1) = second
    r
  }
  //上面的写法其实与下面的写法可以认为是等价的。下面的写法是一种更原生的写法。不建议使用下面的写法
  def arrayMake2[T](first: T, second: T)(implicit m: ClassTag[T]) = {
    val r = new Array[T](2)
    r(0) = first
    r(1) = second
    r
  }

  //implicit m: Manifest[T] 改成implicit m: ClassTage[T]也是可以的
  def manif[T](x: List[T])(implicit m: Manifest[T]) = {
    if(m <:< manifest[String])
      println("List strings")
    else
      println("Some other type")
  }

  def manif2[T](x: List[T])(implicit m: Manifest[T]) = {
    //classManifest比manifest获取信息的能力更弱一点
    if(m <:< classManifest[String])
      println("List strings1111")
    else
      println("Some other type1111")
  }

//  def hadoopFile[K, V, F <: InputFormat[K, V]]
//  (path: String, minPartitions: Int)
//  (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
//    hadoopFile(path,
//      fm.runtimeClass.asInstanceOf[Class[F]],
//      km.runtimeClass.asInstanceOf[Class[K]],
//      vm.runtimeClass.asInstanceOf[Class[V]],
//      minPartitions)
//  }

  //ClassTag是我们最常用的。它主要在运行时指定在编译时无法确定的类别的信息。
  // 我这边 Array[T](elems: _*) 中的下划线是占位符，表示很多元素
  def mkArray[T: ClassTag](elems: T*) = Array[T](elems: _*)

  def main(args: Array[String]) {
    arrayMake(1,2).foreach(println)
    arrayMake2(1,2).foreach(println)
    manif(List("Spark", "Hadoop"))
    manif(List(1, 2))
    manif(List("Scala", 3))
    manif2(List("Spark", "Hadoop"))
    manif2(List(1, 2))
    manif2(List("Scala", 3))

    val m = manifest[MyType[String]]
    println(m)//myscala.scalaexercises.classtag.MyType[java.lang.String]
    val cm = classManifest[MyType[String]]
    println(cm)//myscala.scalaexercises.classtag.MyType[java.lang.String]

    /*
    其实manifest是有问题的，manifest对实际类型的判断会有误(如依赖路径)，所以后来推出了ClassTag和TypeTag，
    用TypeTag来替代manifest，用ClassTag来替代classManifest
    */
    mkArray(1, 2, 3, 4, 5).foreach(println)
  }
}
