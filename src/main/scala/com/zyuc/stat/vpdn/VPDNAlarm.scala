package com.zyuc.stat.vpdn

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-7-31.
  */
object VPDNAlarm {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("OperalogAnalysis")//.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val dayid = sc.getConf.get("spark.app.dayid")




  }



}
