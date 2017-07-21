package com.zyuc.stat.iot.mme

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import com.zyuc.stat.utils.FileUtils.{downloadFileFromHdfs, logInfo, moveNewlogFiles, renameHDFSDir}
import org.apache.spark.sql.DataFrame


/**
  * Created by zhoucw on 17-7-19.
  */
object MMELogETL extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val loadTime = "201707211525"
    // val loadTime = sc.getConf.get("spark.app.loadTime")

    // val inputPath = sc.getConf.get("spark.app.inputPath")
    val inputPath = "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/MME/newlog"
    val outputPath = "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/MME/"

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    // mme日志文件名的通配符
    val hwmmWildcard =  sc.getConf.get("spark.app.HUmmWildcard")
    val hwsmWildcard =  sc.getConf.get("spark.app.HUsmWildcard")
    val ztmmWildcard =  sc.getConf.get("spark.app.ZTmmWildcard")
    val ztsmWildcard =  sc.getConf.get("spark.app.ZTsmWildcard")

    // rename original dir
    val srcLocation = inputPath + "/" + loadTime
    val srcDoingLocation = inputPath + "/" + loadTime + "_doing"
    val isRename = renameHDFSDir(fileSystem, srcLocation, srcDoingLocation)
    var result = "Success"
    if(!isRename){
      result = "Failed"
      logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)
      System.exit(1)
    }
    logInfo(s"$srcLocation rename to $srcDoingLocation :" + result)


    val hwmmLocation =  srcDoingLocation + "/*" + hwmmWildcard + "*"
    val hwsmLocation =  srcDoingLocation + "/*" + hwsmWildcard + "*"
    val ztmmLocation =  srcDoingLocation + "/*" + ztmmWildcard + "*"
    val ztsmLocation =  srcDoingLocation + "/*" + ztsmWildcard + "*"

    val hwmmFileExists = if(fileSystem.globStatus(new Path(hwmmLocation)).size>0) true else false
    val hwsmFileExists = if(fileSystem.globStatus(new Path(hwsmLocation)).size>0) true else false
    val ztmmFileExists = if(fileSystem.globStatus(new Path(ztmmLocation)).size>0) true else false
    val ztsmFileExists = if(fileSystem.globStatus(new Path(ztsmLocation)).size>0) true else false

    if(!hwmmFileExists && !hwsmFileExists && !ztmmFileExists && !ztsmFileExists){
      logInfo("No Files during time: " + loadTime)
      System.exit(1)
    }

    var hwmmDF:DataFrame = null
    var hwsmDF:DataFrame = null
    var ztmmDF:DataFrame = null
    var ztsmDF:DataFrame = null
    var allDF:DataFrame = null
    if(hwmmFileExists){
      hwmmDF = sqlContext.read.format("json").load(hwmmLocation)
      sqlContext.read.format("json").load(hwmmLocation).select("T0").map(x=>x.getString(0) + "abc")
      if(allDF == null){
        allDF = hwmmDF
      }else{
        allDF =  allDF.unionAll(hwmmDF)
      }
    }

    if(hwsmFileExists){
      hwsmDF = sqlContext.read.format("json").load(hwsmLocation).select("T0","T1")
      if(allDF == null){
        allDF = hwsmDF
      }else{
        allDF =  allDF.unionAll(hwsmDF)
      }
    }

    if(ztmmFileExists){
      ztmmDF = sqlContext.read.format("json").load(ztmmLocation).select("T0","T1")
      if(allDF == null) {
        allDF = ztmmDF
      }else {
        allDF =  allDF.unionAll(ztmmDF)
      }
    }

    if(ztsmFileExists){
      ztsmDF = sqlContext.read.format("json").load(ztsmLocation).select("T0","T1")
      if(allDF == null){
        allDF = ztsmDF
      }else {
        allDF =  allDF.unionAll(ztsmDF)
      }
    }




    //val filestatus = fileSystem.globStatus(new Path(outputPath + "newlog/" + "*.csv"))

    //val outputDataPath = outputPath + "data/" + loadTime

   // moveNewlogFiles(outputPath, filestatus,loadTime)
    val hdfsDirLocation = "hdfs:/tmp/zcw/"
    val localDirLocation = "/tmp/zcw1/"
    val suffix = loadTime

    downloadFileFromHdfs(fileSystem, hdfsDirLocation:String, localDirLocation:String, suffix:String)

  }

}
