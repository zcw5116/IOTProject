package com.zyuc.stat.iot.multiana

import com.zyuc.stat.iot.etl.util.CommonETLUtils
import com.zyuc.stat.properties.ConfigProperties
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-8-17.
  * modify by limm 2017/09/04
  */
object ActivedUserAnalysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)

    val appName =  sc.getConf.get("spark.app.name") //
    val pdsnTable = sc.getConf.get("spark.app.table.pdsn")  //iot_cdr_data_pdsn_d 3g
    val pgwTable = sc.getConf.get("spark.app.table.pgw")  //iot_cdr_data_pgw_d   4g
    val haccgTable = sc.getConf.get("spark.app.table.haccg")  //iot_cdr_data_haccg_d  3g
    val userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID")
    val userTable = sc.getConf.get("spark.app.table.userTable") //"iot_customer_userinfo"
    val activeUserTable = sc.getConf.get("spark.app.table.activeUserTable") //"iot_activeduser_data_day"
    val dayid = sc.getConf.get("spark.app.dayid")
    val outputPath = sc.getConf.get("spark.app.outputPath")  //  "hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/activeUser/secondaryoutput/"


    val cdrPartitionID = dayid.substring(2,8)
    //iot_cdr_data_pdsn_d 3g
    val cdrTable = "tmp_cdr_table"
    val cdrDF = sqlContext.sql(
      s"""CACHE TABLE ${cdrTable} as
         |select mdn, '3G' nettype
         |from ${pdsnTable} t
         |where d='${cdrPartitionID}'
         |union all
         |select mdn, '4G' nettype
         |from ${pgwTable} t
         |where d='${cdrPartitionID}'
       """.stripMargin)

    //val raidusTable = s"${appName}_tmp_raidus"
    //sqlContext.sql(
    //  s"""CACHE TABLE ${raidusTable} as
    //     |select mdn, nettype
    //     |from ${radiusTable} t
    //     |where d='${radiusPartitionID}'
    //   """.stripMargin)

    val tmpUserTable = s"${appName}_tmp_user"
    sqlContext.sql(
      s"""CACHE TABLE ${tmpUserTable} as
         |select mdn,
         |case when length(companycode)=0 then 'N999999999' else companycode end  as vpdncompanycode
         |from ${userTable} t
         |where d='${userTablePartitionID}'
       """.stripMargin)

    val activeUserSQL =
      s"""
         |select '${cdrPartitionID}' as d, vpdncompanycode, '3G' nettype, count(*) as activednum
         |from ${tmpUserTable} t left semi join ${cdrTable} r
         |on(t.mdn=r.mdn and r.nettype='3G')
         |group by vpdncompanycode
         |union all
         |select '${cdrPartitionID}' as d, vpdncompanycode, '4G' nettype, count(*) as activednum
         |from ${tmpUserTable} t left semi join ${cdrTable} r
         |on(t.mdn=r.mdn and r.nettype='4G')
         |group by vpdncompanycode
     """.stripMargin

    val resultDF = sqlContext.sql(activeUserSQL)



    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)
    val coalesceNum = 1
    val partitions="d"

    CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, resultDF, coalesceNum, partitions, dayid, outputPath, activeUserTable, appName)

  }
}
