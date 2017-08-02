package com.zyuc.stat.iot.etl

import java.util.Date

import com.zyuc.stat.iot.etl.MMESecondETL.logInfo
import com.zyuc.stat.iot.etl.util.CommonUtils.getTemplate
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.makeCoalesce
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-8-1.
  */
object AuthlogSecondETL extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name")  // name_2017073111
    val inputPath = sc.getConf.get("spark.app.mme.inputPath") //" hdfs://EPC-LOG-NM-15:8020/user/hive/warehouse/iot.db/iot_userauth_3gaaa/"
    val outputPath = sc.getConf.get("spark.app.outputPath")  //"hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/AuthLog/secondETLData/"

    val userTable = sc.getConf.get("spark.app.user.table") //"iot_customer_userinfo"
    val authLogTable = sc.getConf.get("spark.app.firstETL.auth.table") // "iot_userauth_3gaaa"
    val authLogDayHourTable = sc.getConf.get("spark.app.secondETL.auth.table")  // "iot_userauth_3gaaa_day_hour"
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt //128

    val hourid = appName.substring(appName.lastIndexOf("_") + 1)  //"2017073111"

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    // sqlContext.sql("set spark.sql.shuffle.partitions=500")

    val partitionD = hourid.substring(0, 8)
    val partitionH = hourid.substring(8, 10)
    val preDayid = hourid.substring(0, 8)


    // mme第一次清洗保存到位置
    val inputLocation = inputPath + "/d=" + partitionD + "/h=" + partitionH

    import org.apache.spark.sql.functions._
    var begin = new Date().getTime
    // mme第一次清洗保存到位置
    val authDF = sqlContext.table(authLogTable).filter("dayid=" + partitionD).filter("hourid=" + partitionH).
      withColumn("resultflag", when(col("auth_result")===0,"success").otherwise("failed")).
      withColumnRenamed("dayid","d").withColumnRenamed("hourid","h")

    val userDF = sqlContext.table(userTable).filter("d=" + preDayid).
      selectExpr("mdn", "imsicdma", "custprovince", "case when length(vpdncompanycode)=0 then 'N999999999' else vpdncompanycode end  as vpdncompanycode").
      cache()

    // 关联出字段, terminalDF： tac,modelname, devicetype ;  userDF: vpdncompanycode, custprovince
    val resultDF = authDF.join(userDF, userDF.col("imsicdma") === authDF.col("imsicdma") , "left").
      select(
        authDF.col("auth_result"), authDF.col("auth_time"), authDF.col("device"), authDF.col("imsicdma"),
        authDF.col("imsilte"), authDF.col("mdn"), authDF.col("nai_sercode"), authDF.col("nasport"),
        authDF.col("nasportid"), authDF.col("nasporttype"), authDF.col("pcfip"), authDF.col("srcip"),
        userDF.col("vpdncompanycode"), userDF.col("custprovince"),
        authDF.col("d"), authDF.col("h"))


    // 计算cloalesce的数量
    val coalesceNum = makeCoalesce(fileSystem, inputLocation, coalesceSize)
    logInfo(s"$inputPath , $coalesceSize, $coalesceNum")

    // 获取分区模板
    val partitions = "d,h"
    val partitionTemplate = getTemplate(partitions)

    //  将结果保存下来
    resultDF.repartition(coalesceNum).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + hourid)
    logInfo("[" + appName + "] 转换用时 " + (new Date().getTime - begin) + " ms")




  }
}
