package com.zyuc.stat.iot.etl.secondary

import com.zyuc.stat.iot.etl.util.CommonETLUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
  * Created by zhoucw on 17-8-16.
  */
object RadiusSecondETL {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)


    val appName = sc.getConf.get("spark.app.name") // name_2017073111
    val radiusTable = sc.getConf.get("spark.app.table.radiusTable") // pgwradius_out
    val radiusInputPath = sc.getConf.get("spark.app.radiusInputPath")  // hdfs://EPC-IOT-ES-06:8020/user/hive/warehouse/iot.db/pgwradius_out/
    val outputPath = sc.getConf.get("spark.app.outputPath")  //  hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/radius/output/
    val radiusDayid = sc.getConf.get("spark.app.radiusDayid")
    val userTable = sc.getConf.get("spark.app.table.user") //"iot_customer_userinfo"
    val userTablePatitionDayid = sc.getConf.get("spark.app.table.userTablePatitionDayid")
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt //128
    val radiusOutputTable = sc.getConf.get("spark.app.table.radiusOutputTable")

    val userDF = sqlContext.table(userTable).filter("d=" + userTablePatitionDayid).
      selectExpr("mdn", "custprovince", "vpdncompanycode").
      cache()


    val srcLocation = radiusInputPath + "/dayid="+radiusDayid
    val radiusSrcDF = sqlContext.read.format("orc").load(srcLocation)
    val radiusDF = radiusSrcDF.join(userDF, radiusSrcDF.col("mdn")===userDF.col("mdn"), "left").
      select(radiusSrcDF.col("mdn"), radiusSrcDF.col("nettype"), radiusSrcDF.col("time"), radiusSrcDF.col("status"),
        radiusSrcDF.col("terminatecause"),userDF.col("custprovince"), userDF.col("vpdncompanycode")).
      withColumn("d", lit(radiusDayid.substring(2,8)))

    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)
    val coalesceNum = FileUtils.makeCoalesce(fileSystem, srcLocation, coalesceSize.toInt)


    val partitions="d"
    val executeResult = CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, radiusDF, coalesceNum, partitions, radiusDayid, outputPath, radiusOutputTable, appName)

  }

}
