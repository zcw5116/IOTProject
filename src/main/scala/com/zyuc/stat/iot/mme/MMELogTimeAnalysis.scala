package com.zyuc.stat.iot.mme

import com.zyuc.stat.iot.etl.util.MMEConverterUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils.timeCalcWithFormatConvertSafe
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-7-24.
  */
object MMELogTimeAnalysis {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val m5timeid = sc.getConf.get("spark.app.m5timeid") // 201707241500

    val dayid = m5timeid.substring(2,8)
    val hourid = m5timeid.substring(8,10)
    val m5id = m5timeid.substring(10,12)

    val starttimestr = timeCalcWithFormatConvertSafe(m5timeid, "yyyyMMddHHmm", 0, "yyyy-MM-dd HH:mm:ss")
    // sql条件到起始时间
    val startstr = starttimestr + ".000"
    // 根据开始时间获取300秒后的时间字符串
    val endtimestr = timeCalcWithFormatConvertSafe(m5timeid, "yyyyMMddHHmm", 300, "yyyy-MM-dd HH:mm:ss")
    // sql条件的结束时间
    val endstr = endtimestr + ".000"

    val hwsmTypeName = MMEConverterUtils.MME_HWSM_TYPE





  }

}
