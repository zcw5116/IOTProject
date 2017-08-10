package com.zyuc.stat.iot.etl.secondary

import java.util.Date

import com.zyuc.stat.iot.etl.util.CommonETLUtils.getTemplate
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.makeCoalesce
import com.zyuc.stat.utils.{DateUtils, FileUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by zhoucw on 17-8-1.
  */
object MMESecondETL extends Logging {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name")  // name_2017073111
    val inputPath = sc.getConf.get("spark.app.mme.inputPath") //" hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/MME/data/"
    val outputPath = sc.getConf.get("spark.app.outputPath")  //"hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/MME/secondETLData/"
    val terminalTable = sc.getConf.get("spark.app.terminal.table") // "iot_dim_terminal"
    val userTable = sc.getConf.get("spark.app.user.table") //"iot_customer_userinfo"
    val mmeLogTable = sc.getConf.get("spark.app.firstETL.mmelog.table") // "iot_mme_log"
    val mmeLogDayHourTable = sc.getConf.get("spark.app.secondETL.mmelog.table")  // "iot_mme_log_day_hour"
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt //128

    val hourid = appName.substring(appName.lastIndexOf("_") + 1)  //"2017073111"

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    // sqlContext.sql("set spark.sql.shuffle.partitions=500")

    val partitionD = hourid.substring(2, 8)
    val partitionH = hourid.substring(8, 10)
    val preDayid = DateUtils.timeCalcWithFormatConvertSafe("hourid", "yyyymmddHH", -1*24*60*60, "yyyymmdd")
    // mme第一次清洗保存到位置
    val inputLocation = inputPath + "/d=" + partitionD + "/h=" + partitionH
    try {
      var begin = new Date().getTime

      // mme第一次清洗保存到位置
      val mmeDF = sqlContext.table(mmeLogTable).filter("d=" + partitionD).filter("h=" + partitionH)

      // 终端信息表
      val terminalDF = sqlContext.table(terminalTable).select("tac", "modelname", "devicetype").cache() //sqlContext.read.format("orc").load("/hadoop/IOT/ANALY_PLATFORM/BasicData/IOTTerminal/data")

      val userDF = sqlContext.table(userTable).filter("d=" + preDayid).
        selectExpr("mdn", "custprovince", "case when length(vpdncompanycode)=0 then 'N999999999' else vpdncompanycode end  as vpdncompanycode").
        cache()


      // 关联出字段, terminalDF： tac,modelname, devicetype ;  userDF: vpdncompanycode, custprovince
      val resultDF = mmeDF.join(terminalDF, mmeDF.col("imei").substr(0, 8) === terminalDF.col("tac"), "left").
        join(userDF, userDF.col("mdn") === mmeDF.col("msisdn"), "left").
        select(
          mmeDF.col("msisdn").as("mdn"), mmeDF.col("province"), mmeDF.col("imei"), mmeDF.col("procedureid"),
          mmeDF.col("starttime"), mmeDF.col("acctype"), mmeDF.col("imsi"), mmeDF.col("sergw"),
          mmeDF.col("pcause"), mmeDF.col("ci"), mmeDF.col("enbid"), mmeDF.col("uemme"),
          mmeDF.col("newgrpid"), mmeDF.col("newmmecode"), mmeDF.col("newmtmsi"), mmeDF.col("mmetype"),
          mmeDF.col("result"),
          terminalDF.col("tac"), terminalDF.col("modelname"), terminalDF.col("devicetype"),
          userDF.col("vpdncompanycode"), userDF.col("custprovince"), mmeDF.col("d"), mmeDF.col("h"))


      // 计算cloalesce的数量
      val coalesceNum = makeCoalesce(fileSystem, inputLocation, coalesceSize)
      logInfo(s"$inputPath , $coalesceSize, $coalesceNum")

      // 获取分区模板
      val partitions = "d,h"
      val partitionTemplate = getTemplate(partitions)

      //  将结果保存下来
      resultDF.repartition(coalesceNum).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + hourid)
      logInfo("[" + appName + "] 转换用时 " + (new Date().getTime - begin) + " ms")

      begin = new Date().getTime
      val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + hourid + partitionTemplate + "/*.orc"))
      val filePartitions = new mutable.HashSet[String]
      for (i <- 0 until outFiles.length) {
        val nowPath = outFiles(i).getPath.toString
        filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + hourid, "").substring(1))
      }

      FileUtils.moveTempFiles(fileSystem, outputPath, hourid, partitionTemplate, filePartitions)
      logInfo("[" + appName + "] 存储用时 " + (new Date().getTime - begin) + " ms")

      begin = new Date().getTime
      filePartitions.foreach(partition => {
        var d = ""
        var h = ""
        partition.split("/").map(x => {
          if (x.startsWith("d=")) {
            d = x.substring(2)
          }
          if (x.startsWith("h=")) {
            h = x.substring(2)
          }
          null
        })
        if (d.nonEmpty && h.nonEmpty) {
          val sql = s"alter table $mmeLogDayHourTable add IF NOT EXISTS partition(d='$d', h='$h')"
          logInfo(s"partition $sql")
          sqlContext.sql(sql)
        }
      })

      logInfo("[" + appName + "] 刷新分区表用时 " + (new Date().getTime - begin) + " ms")

    } catch {
      case e: Exception =>
        e.printStackTrace()
        SparkHadoopUtil.get.globPath(new Path(outputPath + "temp/" + hourid)).map(fileSystem.delete(_, true))
        logError("[" + appName + "] 失败 处理异常" + e.getMessage)
    } finally {
      sc.stop()
    }

  }

}
