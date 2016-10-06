package org.wq.mvn4flink.datastream.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import scala.collection.JavaConversions._

/**
  * Created by wq on 2016/10/7.
  */
object HBaseNewAPI {
  def main(args: Array[String]) {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "master")


    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    val conn = ConnectionFactory.createConnection(conf)

    //从Connection获得 Admin 对象(相当于以前的 HAdmin)
    val admin = conn.getAdmin

    //本例将操作的表名
    val userTable = TableName.valueOf("user")

    //创建 user 表
    val tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("basic".getBytes))
    println("Creating table `user`. ")
    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)
    println("Done!")

    try{
      //获取 user 表
      val table = conn.getTable(userTable)

      try{
        //准备插入一条 key 为 id001 的数据
        val p = new Put("id001".getBytes)
        //为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
        p.addColumn("basic".getBytes,"name".getBytes, "honest".getBytes)
        //提交
        table.put(p)

        //查询某条数据
        val g = new Get("id001".getBytes)
        val result = table.get(g)
        val value = Bytes.toString(result.getValue("basic".getBytes,"name".getBytes))
        println("GET id001 :"+value)

        //扫描数据
        val s = new Scan()
        s.addColumn("basic".getBytes,"name".getBytes)
        val scanner = table.getScanner(s)

        try{
          for(r <- scanner){
            println("Found row: "+r)
            println("Found value: "+Bytes.toString(r.getValue("basic".getBytes,"name".getBytes)))
          }
        }finally {
          //确保scanner关闭
          scanner.close()
        }

        //删除某条数据,操作方式与 Put 类似
        val d = new Delete("id001".getBytes)
        d.addColumn("basic".getBytes,"name".getBytes)
        table.delete(d)

      }finally {
        if(table != null) table.close()
      }

    }finally {
      conn.close()
    }
  }
}
