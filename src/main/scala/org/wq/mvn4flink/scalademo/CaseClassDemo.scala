package org.wq.mvn4flink.scalademo

/**
 * Created by wq on 14-7-28.
 */
object CaseClassDemo extends App{


  //wqClassCase()
  //wqCaseClass()
  ListCaseClass()

  case class Person(name: String, age: Int) {
    def this() {
      this("", 11)
    }
  }

  class Person1(name: String, age: Int)

  def wqClassCase() {
    val p = new Person1("Mark",34)
    p match {
      case x: Person1 => println("Mark")
    }

  }

  def wqCaseClass(){
    val alice = Person("Alice", 25)
    val bob = Person("Bob",32)
    val charlie = Person("Charlie",32)
    val list = List(alice,bob,charlie)

    list.filter {
      case Person(name,age) => {
        age >25
      }
    }.filter {
      case Person(name,age) => {
        name == "Bob"
      }
    }.map(println _)

  }

  def ListCaseClass(){
    val alice = Person("Alice", 25)
    val bob = Person("Bob",32)
    val charlie = Person("Charlie",32)
    val list = List(alice,bob,charlie)

    for(person <- list){
      person match {
        case Person(name,age) =>{
          if(age > 25){
            println("name:"+name)
            println("age:"+age)
          }
//          if(name == "Bob"){
//            println("name:"+name)
//            println("age:"+age)
//          }
          age > 25 match {
            case x => println("kankan:"+x)
          }
        }
      }

    }


  }

}
