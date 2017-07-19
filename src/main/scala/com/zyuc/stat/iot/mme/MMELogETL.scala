package com.zyuc.stat.iot.mme

import java.util.Date

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import com.zyuc.stat.utils.FileUtils.moveNewlogFiles

import scala.collection.mutable

/**
  * Created by zhoucw on 17-7-19.
  */
object MMELogETL extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val loadTime = "201717191945"

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val outputPath = "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/MME/"

    val filestatus = fileSystem.globStatus(new Path(outputPath + "newlog/" + "*.csv"))

    val outputDataPath = outputPath + "data/" + loadTime

    moveNewlogFiles(outputPath, filestatus,loadTime)


  }

}
