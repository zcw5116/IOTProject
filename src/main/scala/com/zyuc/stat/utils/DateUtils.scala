package com.zyuc.stat.utils

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.commons.lang3.time.FastDateFormat


/**
  * Created by slview on 17-6-27.
  */
object DateUtils {
  def getNowTime(format:String):String={
    val fdf = FastDateFormat.getInstance(format)
    val timeid = fdf.format(new Date())
    timeid
  }

  def getNextday():String= {
  /*  var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, 1)
    var nextday = dateFormat.format(cal.getTime())
    nextday*/

    val fdf = FastDateFormat.getInstance("yyyyMMdd")
    val nexttime = fdf.format(new Date())
    val nextmilis = fdf.parse(nexttime).getTime() + 1*24*60*60*1000
    fdf.format(nextmilis)
  }

  def getNextTime(start_time: String, stepSeconds: Long, format:String) = {
   /* var df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    var begin: Date = df.parse(start_time)
    var endstr: Long = begin.getTime() + stepSeconds * 1000
    var sdf: SimpleDateFormat = new SimpleDateFormat(format)
    var nextTimeStr: String = sdf.format(new Date((endstr)))
    nextTimeStr
   */

    val fdf = FastDateFormat.getInstance("yyyyMMddHHmmss")
    val begin = fdf.parse(start_time)
    val endmilis:Long = begin.getTime() + stepSeconds * 1000
    val targetfdf = FastDateFormat.getInstance(format)
    val nexttimestr = targetfdf.format(endmilis)
    nexttimestr
  }

  def timeCalcWithFormatConvert(sourcetime:String, sourceformat:String, stepseconds:Long, targetformat:String):String = {

    //val fdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    var sourceDF: SimpleDateFormat = new SimpleDateFormat(sourceformat)
    var sourceDate: Date = sourceDF.parse(sourcetime)
    var sourceTime: Long = sourceDate.getTime() + stepseconds*1000
    var targetDF: SimpleDateFormat = new SimpleDateFormat(targetformat)
    var targettime: String = targetDF.format(new Date((sourceTime)))
    targettime
  }

  def timeCalcWithFormatConvertSafe(sourcetime:String, sourceformat:String, stepseconds:Long, targetformat:String):String = {

    val sourceDF:FastDateFormat = FastDateFormat.getInstance(sourceformat)
    val sourceDate:Date = sourceDF.parse(sourcetime)
    val sourceTime: Long = sourceDate.getTime() + stepseconds*1000
    val targetDF:FastDateFormat =  FastDateFormat.getInstance(targetformat)
    val targettime: String = targetDF.format(sourceTime)
    targettime
  }

  def timeInterval(beginTime:String, endTime:String, format:String):Long= {
    val fdf = FastDateFormat.getInstance(format)
    val beginDate:Date = fdf.parse(beginTime)
    val endDate:Date = fdf.parse(endTime)
    val interval = (endDate.getTime() - beginDate.getTime())/1000
    return  interval
  }

  def main(args: Array[String]): Unit = {

    val test = timeInterval("201707271937","201707271938","yyyyMMddHHmm")
    println("test:"+test)

  }
}
