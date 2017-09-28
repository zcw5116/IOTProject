package com.zyuc.stat.iot.analysis.baseline

import com.zyuc.stat.iot.analysis.util.AuthHtableConverter
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.avg
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-9-28.
  */
object AuthBaseLine {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name") // name_2017073111
    val endDayid = sc.getConf.get("spark.app.baseLine.endDayid") // "20170906"
    val intervalDayNums = sc.getConf.get("spark.app.baseLine.intervalDayNums").toInt // “7”
    val hbaseTablePrefix = sc.getConf.get("spark.app.hbaseTablePrefix", "analyze_summ_rst_everycycle_") // iot_cdr_flow_stat_
    val targetdayid = sc.getConf.get("spark.app.htable.targetdayid")

    // 将每天的Hbase数据union all后映射为一个DataFrame
    var hbaseDF:DataFrame = null
    for(i <- 0 until intervalDayNums){
      val dayid = DateUtils.timeCalcWithFormatConvertSafe(endDayid, "yyyyMMdd", -i*24*60*60, "yyyyMMdd")
      if(i == 0){
        hbaseDF = AuthHtableConverter.convertToDF(sc, sqlContext, hbaseTablePrefix + dayid)
      }
      if(i>0){
        hbaseDF = hbaseDF.unionAll(AuthHtableConverter.convertToDF(sc, sqlContext, hbaseTablePrefix + dayid))
      }
    }

    hbaseDF = hbaseDF.select("compnyAndSerAndDomain", "time", "req_3g_ration", "req_4g_ration", "req_vpdn_ration", "req_total_ration")
    val tmpTable = "tmpHbase_" + endDayid
    hbaseDF.registerTempTable(tmpTable)

    //  对每个5分钟点的流量排序, 删除最大值和最小值后取平均值
    val tmpSql =
      s"""
         |select rowkey, compnyAndSerAndDomain, time, req_3g_ration, req_4g_ration,
         |req_vpdn_ration, req_total_ration
         |row_number() over(partition by compnyAndSerAndDomain, time order by req_3g_ration) req3gRN,
         |row_number() over(partition by compnyAndSerAndDomain, time order by req_4g_ration) req4gRN,
         |row_number() over(partition by compnyAndSerAndDomain, time order by req_vpdn_ration) reqVpdnRN,
         |row_number() over(partition by compnyAndSerAndDomain, time order by req_total_ration) reqTotalRN
         |from ${tmpTable}
       """.stripMargin

    val tmpDF = sqlContext.sql(tmpSql).cache()

    var auth3gDF = tmpDF.filter("req3gRN>1").filter("req3gRN<" + intervalDayNums)
    var auth4gDF = tmpDF.filter("req4gRN>1").filter("req4gRN<" + intervalDayNums)
    var authVpdnDF = tmpDF.filter("reqVpdnRN>1").filter("reqVpdnRN<" + intervalDayNums)
    var authTotalDF = tmpDF.filter("reqTotalRN>1").filter("reqTotalRN<" + intervalDayNums)

    if(intervalDayNums <= 2){
      auth3gDF = tmpDF
      auth4gDF = tmpDF
      authVpdnDF = tmpDF
      authTotalDF = tmpDF
    }

    val auth3gRes = auth3gDF.groupBy("rowkey").agg(avg("req_3g_ration")).coalesce(1)
    val auth4gnRes =  auth4gDF.groupBy("rowkey").agg(avg("req_4g_ration")).coalesce(1)
    val authVpdnRes = authVpdnDF.groupBy("rowkey").agg(avg("req_vpdn_ration")).coalesce(1)
    val dauthTotalRes = authTotalDF.groupBy("rowkey").agg(avg("req_total_ration")).coalesce(1)
    
  }

}
