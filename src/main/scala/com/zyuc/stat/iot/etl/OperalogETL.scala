package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.util.OperaLogConverterUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{CharacterEncodeConversion, FileUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

/**
  * Created by zhoucw on 17-7-25.
  */
object OperalogETL extends Logging{
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    // 操作时间
    val operaTime = sc.getConf.get("spark.app.operaTime")  // 20170723
    val pcrfPath = sc.getConf.get("spark.app.pcrfPath") // /hadoop/IOT/ANALY_PLATFORM/OperaLog/PCRF/
    val hssPath = sc.getConf.get("spark.app.hssPath")   // /hadoop/IOT/ANALY_PLATFORM/OperaLog/HSS/
    val hlrPath = sc.getConf.get("spark.app.hlrPath")  //  /hadoop/IOT/ANALY_PLATFORM/OperaLog/HLR/

    val pcrfWildcard = sc.getConf.get("spark.app.pcrfWildcard")  //  *dat*" + operaTime + "*"
    val hssWildcard = sc.getConf.get("spark.app.hssWildcard")   // operaTime + 1
    val hlrWildcard = sc.getConf.get("spark.app.hlrWildcard")  // operaTime => 2017-06-15
    val outputPath = sc.getConf.get("spark.app.outputPath")   // /hadoop/IOT/ANALY_PLATFORM/OperaLog/OutPut/

    val pcrfLocation = pcrfPath + "/" + pcrfWildcard //
    val hssLocation = hssPath + "/" + hssWildcard // operaTime
    val hlrLocation = hlrPath + "/" + hlrWildcard  // operaTime

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val partitions = "d" // 按日分区
    def getTemplate: String = {
      var template = ""
      val partitionArray = partitions.split(",")
      for (i <- 0 until partitionArray.length)
        template = template + "/" + partitionArray(i) + "=*"
      template // rename original dir
    }


    val pcrfFileExists = if (fileSystem.globStatus(new Path(pcrfLocation)).length > 0) true else false
    val hssFileExists = if (fileSystem.globStatus(new Path(hssLocation)).length > 0) true else false
    val hlrFileExists = if (fileSystem.globStatus(new Path(hlrLocation)).length > 0) true else false

    if (!pcrfFileExists && !hssFileExists && !hlrFileExists) {
      logInfo("No Files during time: " + operaTime)
      System.exit(1)
    }

    var operLogDF:DataFrame = null
    if(pcrfFileExists){
      val pcrfTmpDF = sqlContext.read.json(CharacterEncodeConversion.transfer(sc, pcrfLocation, "GBK"))
      val pcrfDF = OperaLogConverterUtils.parse(pcrfTmpDF, OperaLogConverterUtils.Oper_PCRF_PLATFORM)
      if(operLogDF == null){
        operLogDF = pcrfDF
      }else{
        operLogDF = operLogDF.unionAll(pcrfDF)
      }
    }

    if(hssFileExists){
      val hssTmpDF = sqlContext.read.json(CharacterEncodeConversion.transfer(sc, hssLocation, "GBK"))
      val hssDF = OperaLogConverterUtils.parse(hssTmpDF, OperaLogConverterUtils.Oper_HSS_PLATFORM)
      if(operLogDF == null){
        operLogDF = hssDF
      }else{
        operLogDF = operLogDF.unionAll(hssDF)
      }
    }

    if(hlrFileExists){
      val hlrTmpDF = sqlContext.read.json(CharacterEncodeConversion.transfer(sc, hlrLocation, "GBK"))
      val hlrDF = OperaLogConverterUtils.parse(hlrTmpDF, OperaLogConverterUtils.Oper_HLR_PLATFORM)
      if(operLogDF == null){
        operLogDF = hlrDF
      }else{
        operLogDF = operLogDF.unionAll(hlrDF)
      }
    }

    operLogDF.coalesce(1).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + operaTime)
    logInfo(s"write data to temp/${operaTime}")

    val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + operaTime + getTemplate + "/*.orc"))
    val filePartitions = new mutable.HashSet[String]
    for (i <- 0 until outFiles.length) {
      val nowPath = outFiles(i).getPath.toString
      filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + operaTime, "").substring(1))
    }

    FileUtils.moveTempFiles(fileSystem, outputPath, operaTime, getTemplate, filePartitions)
    logInfo(s"moveTempFiles ")

    /*
       scala>   val pcrfTmpDF = sqlContext.read.format("orc").load("hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/OperaLog/OutPut/data")
pcrfTmpDF: org.apache.spark.sql.DataFrame = [platform: string, detailinfo: string, errorinfo: string, imsicdma: string, imsilte: string, mdn: string, netype: string, node: string, operclass: string, opertime: string, opertype: string, oper_result: string, d: int]

 create external table iot_opera_log(platform string, detailinfo string, errorinfo string, imsicdma string, imsilte string, mdn string, netype string, node string, operclass string, opertime string, opertype string, oper_result string) partitioned by (d int)  stored as orc location '/hadoop/IOT/ANALY_PLATFORM/OperaLog/OutPut/data';

     */

    filePartitions.foreach(partition => {
      var d = ""
      partition.split("/").map(x => {
        if (x.startsWith("d=")) {
          d = x.substring(2)
        }
        null
      })
      if (d.nonEmpty) {
        val sql = s"alter table iot_opera_log add IF NOT EXISTS partition(d='$d')"
        logInfo(s"partition $sql")
        sqlContext.sql(sql)
      }
    })

    sc.stop()
  }

}
