package com.zyuc.stat.utils

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * Created by slview on 17-6-29.
  */
object HbaseUtil {
  //创建表
  def createHTable(connection: Connection,tablename: String, familyarr:Array[String]): Unit=
  {
    //Hbase表模式管理器
    val admin = connection.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //如果需要创建表
    if (!admin.tableExists(tableName)) {
      //创建Hbase表模式
      val tableDescriptor = new HTableDescriptor(tableName)

      familyarr.foreach(f => tableDescriptor.addFamily(new HColumnDescriptor(f.getBytes())))
      //创建表
      admin.createTable(tableDescriptor)
      println("create done.")
    }
  }

  // 创建表
  def createIfNotExists(tablename: String, familyarr:Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    val connection = ConnectionFactory.createConnection(conf)
    //Hbase表模式管理器
    val admin = connection.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //如果需要创建表
    if (!admin.tableExists(tableName)) {
      //创建Hbase表模式
      val tableDescriptor = new HTableDescriptor(tableName)
      // 添加列族
      familyarr.foreach(f => tableDescriptor.addFamily(new HColumnDescriptor(f.getBytes())))
      //创建表
      admin.createTable(tableDescriptor)
      println("create done.")
    }
  }

  def main(args: Array[String]): Unit = {
    val tname = "tcrt1"
    val fs = new Array[String](3)
    fs(0) = "test1"
    fs(1) = "test2"
    fs(2) = "test3"
    createIfNotExists(tname, fs)

  }
}
