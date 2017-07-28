package com.zyuc.stat.iot.user

import com.zyuc.stat.iot.etl.util.TerminalConvertUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by LiJian on 2017/7/28.
  */
object IotTerminalAnaly extends Logging{
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val externaleable = "iot_dim_terminal"
    // 如果外部表不存在， 就创建
    val outputPath = sc.getConf.get("spark.app.outputPath")
    val createsql =
      s"""create external table if not exists $externaleable
          |iot_dim_terminal(tac string, marketingname string, manufacturer_applicant string, modelname string, devicetype string, crttime string)
          |stored as orc location $outputPath
       """.stripMargin

    logInfo(s"createsql :  $createsql ")
    sqlContext.sql(createsql)

    val TerminalGroupsql =
      s"""select u.userprovince, u.vpdncompanycode, d.devicetype, d.modelname,count(*) as ter_cnt
          |from iot_user_basic_info  u ,iot_dim_terminal d
          |where substr(u.imei,1,8) = d.tac
          |and length(u.userprovince) > 0
          |and length(u.vpdncompanycode) > 0
          |and length(d.devicetype) > 0
          |and length(d.modelname) > 0
          |group by u.userprovince, u.vpdncompanycode, d.devicetype, d.modelname
       """.stripMargin

    logInfo(s"TerminalGroupsql :  $TerminalGroupsql ")
    val TerminalGroupDF = sqlContext.sql(TerminalGroupsql)

    val textfile = "/hadoop/IOT/ANALY_PLATFORM/BasicData/IOTTerminal/teran"
    TerminalGroupDF.write.mode(SaveMode.Overwrite).format("json").save(textfile)

  }
}
