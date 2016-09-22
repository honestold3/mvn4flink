/**
  * Created by wq on 16/9/21.
  */
class Foo {

  val i = 10

  val j = i

}

class Bar extends {
  override  val i =20
  //override val j = 8
} with Foo

object Main extends App {
  println(new Bar().j)
}
