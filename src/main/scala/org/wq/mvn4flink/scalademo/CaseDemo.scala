package org.wq.mvn4flink.scalademo

/**
 * Created by wq on 14-7-28.
 */
object CaseDemo extends App{

  caseNull
  isEmptyList
  caseList
  caseString
  println(matchTest(1))
  println(matchSome())

  def caseNull(){
    val myMap = Map("key1" -> "value1")

    val value1 = myMap.get("key1")
    val value2 = myMap.get("key2")
    val value3 = myMap.get("key3")

    val result1 = value1.getOrElse(0)
    val result2 = value2.getOrElse(0)
    val result3 = value3 match {
      case Some(n) => n
      case None => 1
    }

    println(result1)
    println(result2)
    println(result3)
  }

  def isEmptyList(){
    val list = List((1,"1"),(2,"2"))
    val list2 = {
      val dd = list.filter{
        case (x: Int,y: String) => x>3  //哈哈在第一个case没有满足时，第二个case要判断，很重要
      }
      dd.isEmpty match {
        case true => 0
        case false => dd.head._1
      }
    }

    println("list2:" + list2)
  }

  def caseList(){
    val list = List(("Mark", 4),("Charles",5),("john",4))
    list.filter{
      case (name,number) => number ==4 && name == "john"
    }.map(println _)

    println("-----------------------------")

    list.filter{
      case (name,number) => number ==4
    }.filter{
      case (name, number) => name == "Mark"
    }.map(println _)
  }

  def caseString(){
    val str = "aa,31,bb,cc"
    val a = str.split(",")
    a match {
      case x: Array[String] => {
        if(a.length ==4){
          println("ww")
        }

      }
    }
  }

  def matchTest(x: Any): Any = x match {
    case 1 => "one"
    case "two" => 2
    case y: Int => "scala.Int"
    case _ => "many"
  }

  def matchSome(x: Any): Any = x match {
    case Some(1) => "one"
    case _ => "haha"
  }

}
