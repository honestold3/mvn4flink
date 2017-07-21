package org.wq.mvn4flink.json

import scala.util.parsing.json.JSON

/**
  * Created by wq on 2017/7/4.
  */
object JsonKan {

  def main(args: Array[String]): Unit = {
    val str2 = "{\"et\":\"kanqiu_client_join\",\"vtm\":1435898329434,\"body\":{\"client\":\"866963024862254\",\"client_type\":\"android\",\"room\":\"NBA_HOME\",\"gid\":\"\",\"type\":\"\",\"roomid\":\"\"},\"time\":1435898329}"

    val b = JSON.parseFull(str2)

//    println(JSON.formatted(b.toString))

    b match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(map: Map[String, Any]) => println(map)
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }

    case class Person (id: Int,name: String,age: Int,addressId:Int)
    case class Address (id: Int, street: String,city: String)

    // no direct reference, to fit with slick database models
    case class PersonWithAddress(person: Person, address: Address)

    val person = Person(0, "John Rambo" , 67, 0)
    val address = Address(0, "101 W Main St", "Madison, Kentucky")
    val pa = PersonWithAddress(person, address)


    sealed trait List[+A] // `List` data type, parameterized on a type, `A`
    case object Nil extends List[Nothing] // A `List` data constructor representing the empty list

    case class Cons[+A](head: A, tail: List[A]) extends List[A]

    @annotation.tailrec
    def foldLeft[A,B](l: List[A], z: B)(f: (B, A) => B): B = l match {
      case Nil => z
      case Cons(h,t) => foldLeft(t, f(z,h))(f)
    }

    def foldRightViaFoldLeft_1[A,B](l: List[A], z: B)(f: (A,B) => B): B =
      foldLeft(l, (b:B) => b)((g,a) => b => g(f(a,b)))(z)



  }

}
