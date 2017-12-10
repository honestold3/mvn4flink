package org.wq.mvn4flink.dynamicExecution

import javax.script.ScriptEngineManager
import javax.script.ScriptEngine

object dynamic {

  def main(args: Array[String]): Unit = {

    val m = new ScriptEngineManager()
    val engine = m.getEngineByName("scala")

    //需要设计的属性
    val settings = engine.asInstanceOf[scala.tools.nsc.interpreter.IMain].settings
    settings.usejavacp.value = true  //使用程序的class path作为engine的class path

    engine.put("m", 10)
    engine.eval("1 to m.asInstanceOf[Int] foreach println")

  }

}
