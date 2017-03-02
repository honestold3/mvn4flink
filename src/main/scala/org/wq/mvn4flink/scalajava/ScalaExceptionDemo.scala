package org.wq.mvn4flink.scalajava

import java.io.{File, IOException}

/**
  * Created by wq on 2016/10/11.
  */
object ScalaExceptionDemo extends App{
  val file: File = new File("a.txt")
  if (!file.exists) {
    try {
      file.createNewFile
    }
    catch {
      //通过模式匹配来实现异常处理
      case e: IOException => {
        e.printStackTrace
      }
    }
  }
}

class ScalaThrower {
  //Scala利用注解@throws声明抛出异常
  @throws(classOf[Exception])
  def exceptionThrower {
    throw new Exception("Exception!")
  }
}
