package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.util.MMEConverterUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import com.zyuc.stat.utils.FileUtils.renameHDFSDir
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by zhoucw on 17-7-19.
  */
object MMELogETL extends Logging {

  def doJob(parentContext: SQLContext, fileSystem:FileSystem, appName:String, loadTime:String, inputPath:String, outputPath:String, hwmmWildcard:String, hwsmWildcard:String, ztmmWildcard:String, ztsmWildcard:String): String ={
    var sqlContext = parentContext.newSession()

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    //val loadTime = "201707211525"
/*    val loadTime = sc.getConf.get("spark.app.loadTime")
    val inputPath = sc.getConf.get("spark.app.inputPath")
    val outputPath = sc.getConf.get("spark.app.outputPath")
    // val inputPath = "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/MME/newlog"
    // val outputPath = "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/MME/"

    // mme日志文件名的通配符
    val hwmmWildcard = sc.getConf.get("spark.app.HUmmWildcard")
    val hwsmWildcard = sc.getConf.get("spark.app.HUsmWildcard")
    val ztmmWildcard = sc.getConf.get("spark.app.ZTmmWildcard")
    val ztsmWildcard = sc.getConf.get("spark.app.ZTsmWildcard")*/

    /*
      val hwmmWildcard =  "HuaweiUDN-MM"
      val hwsmWildcard =  "HuaweiUDN-SM"
      val ztmmWildcard =  "sgsnmme_mm"
      val ztsmWildcard =  "sgsnmme_sm"
   */


    val fastDateFormat = FastDateFormat.getInstance("yyMMddHHmm")


    val partitions = "d,h,m5"

    def getTemplate: String = {
      var template = ""
      val partitionArray = partitions.split(",")
      for (i <- 0 until partitionArray.length)
        template = template + "/" + partitionArray(i) + "=*"
      template // rename original dir
    }
    try {
    val srcLocation = inputPath + "/" + loadTime
    val srcDoingLocation = inputPath + "/" + loadTime + "_doing"
    val isRename = renameHDFSDir(fileSystem, srcLocation, srcDoingLocation)
    var result = "Success"
    if (!isRename) {
      result = "Failed"
      logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)
      return "appName:" + appName + ": " + s"$srcLocation rename to $srcDoingLocation :" + result + ". "
    }
    logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)


    val hwmmLocation = srcDoingLocation + "/*" + hwmmWildcard + "*"
    val hwsmLocation = srcDoingLocation + "/*" + hwsmWildcard + "*"
    val ztmmLocation = srcDoingLocation + "/*" + ztmmWildcard + "*"
    val ztsmLocation = srcDoingLocation + "/*" + ztsmWildcard + "*"

    val hwmmFileExists = if (fileSystem.globStatus(new Path(hwmmLocation)).length > 0) true else false
    val hwsmFileExists = if (fileSystem.globStatus(new Path(hwsmLocation)).length > 0) true else false
    val ztmmFileExists = if (fileSystem.globStatus(new Path(ztmmLocation)).length > 0) true else false
    val ztsmFileExists = if (fileSystem.globStatus(new Path(ztsmLocation)).length > 0) true else false

    if (!hwmmFileExists && !hwsmFileExists && !ztmmFileExists && !ztsmFileExists) {
      logInfo("No Files during time: " + loadTime)
      // System.exit(1)
      return "appName:" + appName + ":No Files ."
    }

    var hwmmDF: DataFrame = null
    var hwsmDF: DataFrame = null
    var ztmmDF: DataFrame = null
    var ztsmDF: DataFrame = null
    var allDF: DataFrame = null

    if (hwmmFileExists) {
      val srchwmmDF = sqlContext.read.format("json").load(hwmmLocation)
      hwmmDF = MMEConverterUtils.parseMME(srchwmmDF, MMEConverterUtils.MME_HWMM_TYPE)
      if (allDF == null) {
        allDF = hwmmDF
      } else {
        allDF = allDF.unionAll(hwmmDF)
      }
    }

    if (hwsmFileExists) {
      val srchwsmDF = sqlContext.read.format("json").load(hwsmLocation)
      hwsmDF = MMEConverterUtils.parseMME(srchwsmDF, MMEConverterUtils.MME_HWSM_TYPE)
      if (allDF == null) {
        allDF = hwsmDF
      } else {
        allDF = allDF.unionAll(hwsmDF)
      }
    }

    if (ztmmFileExists) {
      val srcztmmDF = sqlContext.read.format("json").load(ztmmLocation)
      ztmmDF = MMEConverterUtils.parseMME(srcztmmDF, MMEConverterUtils.MME_ZTMM_TYPE)
      if (allDF == null) {
        allDF = ztmmDF
      } else {
        allDF = allDF.unionAll(ztmmDF)
      }
    }

    if (ztsmFileExists) {
      val srcztsmDF = sqlContext.read.format("json").load(ztsmLocation)
      ztsmDF = MMEConverterUtils.parseMME(srcztsmDF, MMEConverterUtils.MME_ZTSM_TYPE)
      if (allDF == null) {
        allDF = ztsmDF
      } else {
        allDF = allDF.unionAll(ztsmDF)
      }
    }

    allDF.coalesce(1).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + loadTime)
    logInfo(s"write data to temp/${loadTime}")


    val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + loadTime + getTemplate + "/*.orc"))
    val filePartitions = new mutable.HashSet[String]
    for (i <- 0 until outFiles.length) {
      val nowPath = outFiles(i).getPath.toString
      filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + loadTime, "").substring(1))
    }

    FileUtils.moveTempFiles(fileSystem, outputPath, loadTime, getTemplate, filePartitions)
    logInfo(s"moveTempFiles ")

    /*
        create external table iot_mme_log(procedureid string, starttime string, acctype string, IMSI string, MSISDN string, sergw string, pcause string, imei string, ci string, eNBID string, uemme string, newgrpid string, newmmecode string, newmtmsi string, oldmcc string, oldgrpid string, oldmmecode string, oldmtmsi string, province string, mmetype string) partitioned by (d int, h int, m5 int) stored as orc location '/hadoop/IOT/ANALY_PLATFORM/MME/data';
    */

    filePartitions.foreach(partition => {
      var d = ""
      var h = ""
      var m5 = ""
      partition.split("/").map(x => {
        if (x.startsWith("d=")) {
          d = x.substring(2)
        }
        if (x.startsWith("h=")) {
          h = x.substring(2)
        }
        if (x.startsWith("m5=")) {
          m5 = x.substring(3)
        }
        null
      })
      if (d.nonEmpty && h.nonEmpty && m5.nonEmpty) {
        val sql = s"alter table iot_mme_log add IF NOT EXISTS partition(d='$d', h='$h',m5='$m5')"
        logInfo(s"partition $sql")
        sqlContext.sql(sql)
      }
    })


    val srcDoneLocation = inputPath + "/" + loadTime + "_done"
    val isDoneRename = renameHDFSDir(fileSystem, srcDoingLocation, srcDoneLocation)
    var doneResult = "Success"
    if (!isDoneRename) {
      doneResult = "Failed"
      logInfo(s"$srcDoingLocation rename to $srcDoneLocation :" + result)
      return "appName:" + appName + ":" + s"$srcDoingLocation rename to $srcDoneLocation :" + result + ". "
      //System.exit(1)
    }
    logInfo(s"$srcDoingLocation rename to $srcDoneLocation :" + result)

    return "appName:" + appName + ": ETL Success. "

    }catch {
      case e: Exception =>
        e.printStackTrace()
        SparkHadoopUtil.get.globPath(new Path(outputPath + "temp/" + loadTime)).map(fileSystem.delete(_, true))
        SparkHadoopUtil.get.globPath(new Path(outputPath + "temp/" + loadTime)).map(fileSystem.delete(_, false))
        logError("[" + appName + "] 失败 处理异常" + e.getMessage)
        return "appName:" + appName + ": ETL Failed. "
    }
  }


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    //val loadTime = "201707211525"
    val loadTime = sc.getConf.get("spark.app.loadTime")
    val inputPath = sc.getConf.get("spark.app.inputPath")
    val outputPath = sc.getConf.get("spark.app.outputPath")
    // val inputPath = "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/MME/newlog"
    // val outputPath = "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/MME/"

    // mme日志文件名的通配符
    val hwmmWildcard = sc.getConf.get("spark.app.HUmmWildcard")
    val hwsmWildcard = sc.getConf.get("spark.app.HUsmWildcard")
    val ztmmWildcard = sc.getConf.get("spark.app.ZTmmWildcard")
    val ztsmWildcard = sc.getConf.get("spark.app.ZTsmWildcard")

    /*
      val hwmmWildcard =  "HuaweiUDN-MM"
      val hwsmWildcard =  "HuaweiUDN-SM"
      val ztmmWildcard =  "sgsnmme_mm"
      val ztsmWildcard =  "sgsnmme_sm"
   */


    val fastDateFormat = FastDateFormat.getInstance("yyMMddHHmm")
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)


    val partitions = "d,h,m5"

    def getTemplate: String = {
      var template = ""
      val partitionArray = partitions.split(",")
      for (i <- 0 until partitionArray.length)
        template = template + "/" + partitionArray(i) + "=*"
      template // rename original dir
    }



    val srcLocation = inputPath + "/" + loadTime
    val srcDoingLocation = inputPath + "/" + loadTime + "_doing"
    val isRename = renameHDFSDir(fileSystem, srcLocation, srcDoingLocation)
    var result = "Success"
    if (!isRename) {
      result = "Failed"
      logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)
      System.exit(1)
    }
    logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)


    val hwmmLocation = srcDoingLocation + "/*" + hwmmWildcard + "*"
    val hwsmLocation = srcDoingLocation + "/*" + hwsmWildcard + "*"
    val ztmmLocation = srcDoingLocation + "/*" + ztmmWildcard + "*"
    val ztsmLocation = srcDoingLocation + "/*" + ztsmWildcard + "*"

    val hwmmFileExists = if (fileSystem.globStatus(new Path(hwmmLocation)).length > 0) true else false
    val hwsmFileExists = if (fileSystem.globStatus(new Path(hwsmLocation)).length > 0) true else false
    val ztmmFileExists = if (fileSystem.globStatus(new Path(ztmmLocation)).length > 0) true else false
    val ztsmFileExists = if (fileSystem.globStatus(new Path(ztsmLocation)).length > 0) true else false

    if (!hwmmFileExists && !hwsmFileExists && !ztmmFileExists && !ztsmFileExists) {
      logInfo("No Files during time: " + loadTime)
      System.exit(1)
    }

    var hwmmDF: DataFrame = null
    var hwsmDF: DataFrame = null
    var ztmmDF: DataFrame = null
    var ztsmDF: DataFrame = null
    var allDF: DataFrame = null

    if (hwmmFileExists) {
      val srchwmmDF = sqlContext.read.format("json").load(hwmmLocation)
      hwmmDF = MMEConverterUtils.parseMME(srchwmmDF, MMEConverterUtils.MME_HWMM_TYPE)
      if (allDF == null) {
        allDF = hwmmDF
      } else {
        allDF = allDF.unionAll(hwmmDF)
      }
    }

    if (hwsmFileExists) {
      val srchwsmDF = sqlContext.read.format("json").load(hwsmLocation)
      hwsmDF = MMEConverterUtils.parseMME(srchwsmDF, MMEConverterUtils.MME_HWSM_TYPE)
      if (allDF == null) {
        allDF = hwsmDF
      } else {
        allDF = allDF.unionAll(hwsmDF)
      }
    }

    if (ztmmFileExists) {
      val srcztmmDF = sqlContext.read.format("json").load(ztmmLocation)
      ztmmDF = MMEConverterUtils.parseMME(srcztmmDF, MMEConverterUtils.MME_ZTMM_TYPE)
      if (allDF == null) {
        allDF = ztmmDF
      } else {
        allDF = allDF.unionAll(ztmmDF)
      }
    }

    if (ztsmFileExists) {
      val srcztsmDF = sqlContext.read.format("json").load(ztsmLocation)
      ztsmDF = MMEConverterUtils.parseMME(srcztsmDF, MMEConverterUtils.MME_ZTSM_TYPE)
      if (allDF == null) {
        allDF = ztsmDF
      } else {
        allDF = allDF.unionAll(ztsmDF)
      }
    }

    allDF.coalesce(1).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + loadTime)
    logInfo(s"write data to temp/${loadTime}")


    val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + loadTime + getTemplate + "/*.orc"))
    val filePartitions = new mutable.HashSet[String]
    for (i <- 0 until outFiles.length) {
      val nowPath = outFiles(i).getPath.toString
      filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + loadTime, "").substring(1))
    }

    FileUtils.moveTempFiles(fileSystem, outputPath, loadTime, getTemplate, filePartitions)
    logInfo(s"moveTempFiles ")

    /*
        create external table iot_mme_log(procedureid string, starttime string, acctype string, IMSI string, MSISDN string, sergw string, pcause string, imei string, ci string, eNBID string, uemme string, newgrpid string, newmmecode string, newmtmsi string, oldmcc string, oldgrpid string, oldmmecode string, oldmtmsi string, province string, mmetype string) partitioned by (d int, h int, m5 int) stored as orc location '/hadoop/IOT/ANALY_PLATFORM/MME/data';
    */

    filePartitions.foreach(partition => {
      var d = ""
      var h = ""
      var m5 = ""
      partition.split("/").map(x => {
        if (x.startsWith("d=")) {
          d = x.substring(2)
        }
        if (x.startsWith("h=")) {
          h = x.substring(2)
        }
        if (x.startsWith("m5=")) {
          m5 = x.substring(3)
        }
        null
      })
      if (d.nonEmpty && h.nonEmpty && m5.nonEmpty) {
        val sql = s"alter table iot_mme_log add IF NOT EXISTS partition(d='$d', h='$h',m5='$m5')"
        logInfo(s"partition $sql")
        sqlContext.sql(sql)
      }
    })


    val srcDoneLocation = inputPath + "/" + loadTime + "_done"
    val isDoneRename = renameHDFSDir(fileSystem, srcDoingLocation, srcDoneLocation)
    var doneResult = "Success"
    if (!isDoneRename) {
      doneResult = "Failed"
      logInfo(s"$srcDoingLocation rename to $srcDoneLocation :" + result)
      System.exit(1)
    }
    logInfo(s"$srcDoingLocation rename to $srcDoneLocation :" + result)
   sc.stop()
  }

}
