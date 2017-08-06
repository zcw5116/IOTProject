package test

import com.zyuc.stat.utils.DateUtils
import com.zyuc.stat.utils.DateUtils.timeCalcWithFormatConvertSafe
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by slview on 17-7-11.
  */
object WordCount {
  def main (args: Array[String]): Unit = {

    val beginMinu = "201708021400"
    val endMinu = "201708031400"
    val in = DateUtils.timeInterval(beginMinu.substring(0,8), endMinu.substring(0,8), "yyyymmdd")/(24*3600)
    println("in:" + in)

  }

}