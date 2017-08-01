package com.zyuc.stat.vpdn

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-7-31.
  */
object VPDNAlarm {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("OperalogAnalysis")//.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val dayid = sc.getConf.get("spark.app.dayid")
    val preDayid = DateUtils.timeCalcWithFormatConvertSafe(dayid, "yyyymmdd", -1*24*60*60, "yyyymmdd")
    val mmePartionDay = dayid.substring(2,8)

    // 缓存前一天的用户数据
    val cachedUserinfoTable = "iot_user_basic_info_cached"
    sqlContext.sql(
      s"""CACHE TABLE ${cachedUserinfoTable} as
         |select u.mdn, u.custprovince,
         |       case when length(u.vpdncompanycode)=0 then 'N999999999' else u.vpdncompanycode end  as vpdncompanycode
         |from iot_customer_userinfo u where u.d='${preDayid}'
       """.stripMargin)
    // 缓存终端型号的表
    val cachedTermialTable = "cachedTermialTable"
    sqlContext.sql(s"CACHE TABLE ${cachedTermialTable} as select tac, devicetype, modelname from iot_dim_terminal ")

    // 失败原因
    val failedSql =
      s"""select  m.province, u.vpdncompanycode, m.pcause, count(*) as req_cnt
         |from iot_mme_log m, ${cachedUserinfoTable} u, ${cachedTermialTable} t
         |where m.d=${mmePartionDay}
         |and m.msisdn = u.mdn and m.pcause<>'success'
         |group by m.province, u.vpdncompanycode, m.pcause
       """.stripMargin

    sqlContext.sql(failedSql)



  }



}

















