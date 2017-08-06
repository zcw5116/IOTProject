package com.zyuc.stat.iot.etl

import com.zyuc.stat.iot.etl.AuthlogSecondETL.logInfo
import com.zyuc.stat.iot.etl.MMELogETL.logInfo
import com.zyuc.stat.iot.etl.util.{AuthLogConverterUtils, CommonETLUtils}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.{makeCoalesce, renameHDFSDir}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-8-6.
  */
object AuthLogETL extends Logging{

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)


    // val loadTime = sc.getConf.get("spark.app.loadTime")
    val loadTime = "201708071205"
    // val inputPath = sc.getConf.get("spark.app.inputPath") // /hadoop/IOT/ANALY_PLATFORM/AuthLog/ETL/SrcData/auth3gAAA
    // val outputPath = sc.getConf.get("spark.app.outputPath") // /hadoop/IOT/ANALY_PLATFORM/AuthLog/ETL/auth3gAAA
    // val inputPath = "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/AuthLog/ETL/SrcData/auth3gAAA/"
    // val outputPath = "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/AuthLog/ETL/auth3gAAA/"
    val inputPath = "hdfs://cdh-nn1:8020/hadoop/IOT/data/auth/auth3g/srcdata/"
    val outputPath = "hdfs://cdh-nn1:8020/hadoop/IOT/data/auth/auth3g/output/"

    val appName="auth3gaaa_" + loadTime

    // mme日志文件名的通配符
    // val fileWildcard = sc.getConf.get("spark.app.HUmmWildcard")
    val fileWildcard = "3gaaa*"

    val coalesceSize = 128

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    doJob(sc, sqlContext, fileSystem, appName, loadTime, inputPath, outputPath, fileWildcard, coalesceSize)


  }

  def doJob(sc:SparkContext, parentContext: HiveContext, fileSystem:FileSystem, appName:String, loadTime:String, inputPath:String, outputPath:String, fileWildcard:String, coalesceSize:Int): String = {
    var sqlContext = parentContext.newSession()

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val srcLocation = inputPath + "/" + loadTime

    //  val fileExists = if (fileSystem.globStatus(new Path(srcLocation + "/*")).length > 0) true else false
    val fileExists = true
    if(!fileExists) {
      logInfo(s"$srcLocation not exists.")
      return s"$srcLocation not exists."
    }

    val srcDoingLocation = inputPath + "/" + loadTime + "_doing"
    //val isRename = renameHDFSDir(fileSystem, srcLocation, srcDoingLocation)
    val isRename=true
    var result = "Success"
    if (!isRename) {
      result = "Failed"
      logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)
      return "appName:" + appName + ": " + s"$srcLocation rename to $srcDoingLocation :" + result + ". "
    }
    logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)

    val authLocation = srcDoingLocation + "/" + fileWildcard
    val authFileExists = if (fileSystem.globStatus(new Path(authLocation)).length > 0) true else false

    if (!authFileExists) {
      logInfo("No Files during time: " + loadTime)
      // System.exit(1)
      return "appName:" + appName + ":No Files ."
    }

    val srcAuthDF = sqlContext.read.format("json").load(authLocation)

    val authDF = AuthLogConverterUtils.parse(srcAuthDF, AuthLogConverterUtils.LOG_TYPE_3G)
    authDF.show(200)

    // 计算cloalesce的数量
    val coalesceNum = makeCoalesce(fileSystem, srcDoingLocation, coalesceSize)
    logInfo(s"$inputPath , $coalesceSize, $coalesceNum")

    val testTable="iot_auth_data_3gaaa"

    // 结果数据分区字段
    val partitions = "d,h,m5"
    // 将数据存入到HDFS， 并刷新分区表
    CommonETLUtils.saveDFtoPartition(sc, sqlContext, fileSystem, authDF, coalesceNum, partitions, loadTime, outputPath, testTable, appName)


    "test"
  }

}
