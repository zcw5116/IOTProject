package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * 认证日志实时分析
  *
  * @author zhoucw
  * @version 1.0
  *
  */
object AuthRealtimeAnalysis {
  /**
    * 主函数
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    // 获取参数
    val appName = sc.getConf.get("spark.app.name","name_201708010040") // name_201708010040
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo") //
    val userAndDomainTable = sc.getConf.get("spark.app.table.userAndDomainTable", "iot_basic_user_and_domain")
    val companyAndDomainTable = sc.getConf.get("spark.app.table.companyAndDomainTable", "iot_basic_company_and_domain")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid")

    val auth3gTable = sc.getConf.get("spark.app.table.auth3gTable", "iot_userauth_3gaaa")
    val auth4gTable = sc.getConf.get("spark.app.table.auth4gTable", "iot_userauth_4gaaa")
    val authVPDNTable = sc.getConf.get("spark.app.table.authVPDNTable", "iot_userauth_vpdn")


    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val startTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5*60, "yyyyMMddHHmm")
    val endTime = dataTime
    val startTimeStr = DateUtils.timeCalcWithFormatConvertSafe(startTime, "yyyyMMddHHmm", 0, "yyyy-MM-dd HH:mm:ss")
    val endTimeStr = DateUtils.timeCalcWithFormatConvertSafe(endTime, "yyyyMMddHHmm", 0, "yyyy-MM-dd HH:mm:ss")
    val partitionD = startTime.substring(0, 8)
    val partitionH = startTime.substring(8, 10)

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val userInfoTableCached = "userInfoTableCached"
    sqlContext.sql(s"cache table ${userInfoTableCached} as select mdn, companycode, isvpdn, isdirect from $userInfoTable where d=$userTableDataDayid")


    // 关联3g的mdn, domain,  基站
    val mdnSql =
      s"""
         |select '3g' type, u.mdn,
         |       regexp_extract(a.nai_sercode, '^.+@(.*vpdn.*)', 1) as domain,
         |       a.auth_result, a.nasportid,
         |       case when a.auth_result = 0 then 's' else 'f' end as result
         |from  ${userInfoTableCached} u, ${auth3gTable} a
         |where a.dayid = '${partitionD}'  and a.hourid = '${partitionH}'
         |      and u.d='${userTableDataDayid}' and u.imsicdma = a.imsicdma
         |      and a.auth_time >= '${startTimeStr}' and a.auth_time < '${endTimeStr}'
         |union all
         |select '4g' type, a.mdn,
         |       regexp_extract(a.nai_sercode, '^.+@(.*vpdn.*)', 1) as domain,
         |       a.auth_result, a.nasportid,
         |       case when a.auth_result = 0 then 's' else 'f' end as result
         |from ${auth4gTable} a
         |where a.dayid = '${partitionD}'  and a.hourid = '${partitionH}'
         |      and a.auth_time >= '${startTimeStr}' and a.auth_time < '${endTimeStr}'
         |union all
         |select 'vpdn' type, a.mdn,
         |       regexp_extract(a.nai_sercode, '^.+@(.*vpdn.*)', 1) as domain,
         |       a.auth_result, a.nasportid,
         |       case when a.auth_result = 1 then 's' else 'f' end as result
         |from ${authVPDNTable} a
         |where a.dayid = '${partitionD}'  and a.hourid = '${partitionH}'
         |      and a.auth_time >= '${startTimeStr}' and a.auth_time < '${endTimeStr}'
       """.stripMargin
    val mdnTable = "mdnTable_" + startTime
    sqlContext.sql(mdnSql).registerTempTable(mdnTable)


    //  统计普通业务/定向业务/总的业务数据
    // d_request_cnt: 定向业务请求数, d_request_f_cnt: 定向业务请求失败数
    // c_request_cnt: 普通业务请求数, c_request_f_cnt: 普通业务请求失败数
    // t_request_cnt: 总请求数,      t_request_f_cnt: 总请求失败数
    val statSql =
      s"""
         |select u.companycode, t.type,
         |sum(case when u.isdirect=1 then 1 else 0 end) as d_request_cnt,
         |sum(case when u.isdirect=1 and t.result='f' then 1 else 0 end) as d_request_f_cnt,
         |sum(case when u.isdirect=0 then 1 else 0 end) as c_request_cnt,
         |sum(case when u.isdirect=0 and t.result='f' then 1 else 0 end) as c_request_f_cnt,
         |count(*) as t_request_cnt,
         |sum(case when t.result='f' then 1 else 0 end) as t_request_f_cnt
         |from ${mdnTable} t, ${userInfoTableCached} u
         |where t.mdn = u.mdn
         |group by u.companycode, t.type
       """.stripMargin



  }

}
