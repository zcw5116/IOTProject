package com.zyuc.stat.iot.etl.util

/**
  * Created by zhoucw on 17-8-1.
  */
object CommonUtils {


  // 根据分区字符串生成分区模板, example: partitions = "d,h"  return:  /d=*/h=*
  def getTemplate(partitions:String): String = {
    var template = ""
    val partitionArray = partitions.split(",")
    for (i <- 0 until partitionArray.length)
      template = template + "/" + partitionArray(i) + "=*"
    template // rename original dir
  }
}
