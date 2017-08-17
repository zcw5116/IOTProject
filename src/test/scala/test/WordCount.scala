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

    val calPeriodHourNum  = "2"
    var hourid="00"
    for(i <- 0 until 24/calPeriodHourNum.toInt){
      hourid = DateUtils.timeCalcWithFormatConvertSafe(hourid, "HH", (calPeriodHourNum.toInt)*3600, "HH")

      println(hourid)

    }

  }

}