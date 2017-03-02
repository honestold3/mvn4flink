package org.wq.mvn4flink.scalajava

import java.util._
/**
  * Created by wq on 2016/10/11.
  */
case class Person(val name:String,val age:Int)
//Java：Comparator<Person>
//Scala：Comparator[Person]
class PersonComparator extends Comparator[Person]{
  override def compare(o1: Person, o2: Person): Int = if(o1.age>o2.age) 1 else -1
}
object ScalaUseJavaComparator extends  App{
  val p1=Person("li",27)
  val p2=Person("zhang",29)
  val personComparator=new PersonComparator()
  if(personComparator.compare(p1,p2)>0) println(p1)
  else println(p2)

}
