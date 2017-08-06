package com.zyuc.stat.iot.etl.util

import java.util.Date

import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

/**
  * Created by zhoucw on 17-8-1.
  */
object CommonETLUtils extends Logging {


  // 根据分区字符串生成分区模板, example: partitions = "d,h"  return:  /d=*/h=*
  def getTemplate(partitions: String): String = {
    var template = ""
    val partitionArray = partitions.split(",")
    for (i <- 0 until partitionArray.length)
      template = template + "/" + partitionArray(i) + "=*"
    template // rename original dir
  }


  def saveDFtoPartition(sc: SparkContext, sqlContext: HiveContext, fileSystem: FileSystem, df: DataFrame, coalesceNum: Int, partitions: String, loadtime: String, outputPath: String, partitonTable: String, appName: String) = {

    try {
      var begin = new Date().getTime

      val partitionTemplate = getTemplate(partitions)
      df.repartition(coalesceNum).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath + "temp/" + loadtime)
      logInfo("[" + appName + "] 转换用时 " + (new Date().getTime - begin) + " ms")

      begin = new Date().getTime
      val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + loadtime + partitionTemplate + "/*.orc"))
      val filePartitions = new mutable.HashSet[String]
      for (i <- 0 until outFiles.length) {
        val nowPath = outFiles(i).getPath.toString
        filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + loadtime, "").substring(1))
      }

      FileUtils.moveTempFiles(fileSystem, outputPath, loadtime, partitionTemplate, filePartitions)
      logInfo("[" + appName + "] 存储用时 " + (new Date().getTime - begin) + " ms")

      begin = new Date().getTime
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
        var sql = ""
        if (d.nonEmpty && h.nonEmpty && m5.nonEmpty) {
          sql = s"alter table $partitonTable add IF NOT EXISTS partition(d='$d', h='$h', m5='$m5')"
        }
        else if (d.nonEmpty && h.nonEmpty) {
          sql = s"alter table $partitonTable add IF NOT EXISTS partition(d='$d', h='$h')"
        } else if (d.nonEmpty) {
          sql = s"alter table $partitonTable add IF NOT EXISTS partition(d='$d')"
        }
        logInfo(s"partition $sql")
        if (sql.nonEmpty) {
          sqlContext.sql(sql)
        }
      })
      logInfo("[" + appName + "] 刷新分区表用时 " + (new Date().getTime - begin) + " ms")
    } catch {
      case e: Exception => {
        e.printStackTrace()
        SparkHadoopUtil.get.globPath(new Path(outputPath + "temp/" + loadtime)).map(fileSystem.delete(_, true))
        logError("[" + appName + "] 失败 处理异常" + e.getMessage)
      }
    }finally {

    }

  }


}
