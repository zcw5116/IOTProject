package com.zyuc.stat.iot.mme

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import com.zyuc.stat.utils.FileUtils.{downloadFileFromHdfs, moveNewlogFiles, renameHDFSDir}


/**
  * Created by zhoucw on 17-7-19.
  */
object MMELogETL extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val loadTime = "201717191945"
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
    val destLocation = inputPath + "/" + loadTime + "_doing"
    renameHDFSDir(fileSystem, srcLocation, destLocation)



    //val filestatus = fileSystem.globStatus(new Path(outputPath + "newlog/" + "*.csv"))

    //val outputDataPath = outputPath + "data/" + loadTime

   // moveNewlogFiles(outputPath, filestatus,loadTime)
    val hdfsDirLocation = "hdfs:/tmp/zcw/"
    val localDirLocation = "/tmp/zcw1/"
    val suffix = loadTime

    downloadFileFromHdfs(fileSystem, hdfsDirLocation:String, localDirLocation:String, suffix:String)

  }

}
