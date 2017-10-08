package com.zyuc.stat.iot.user

import com.zyuc.stat.iot.user.UserOnlineBaseData.logError
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils.timeCalcWithFormatConvertSafe
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-10-8.
  */
object OnlineBase extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)


    val curHourtime = sc.getConf.get("spark.app.hourid") // 2017072412
    val outputPath = sc.getConf.get("spark.app.outputPath") // "/hadoop/IOT/ANALY_PLATFORM/UserOnline/"
    val userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID")
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid", "20170922")
    val pdsnTable = sc.getConf.get("spark.app.table.pdsnTable", "iot_cdr_data_pdsn")
    val pgwTable = sc.getConf.get("spark.app.table.pgwTable", "iot_cdr_data_pgw")
    val haccgTable = sc.getConf.get("spark.app.table.haccgTable", "iot_cdr_data_haccg")
    val basenumTable = sc.getConf.get("spark.app.table.basenumTable", "iot_useronline_base_nums")
    val whetherUpdateBaseDataTime = sc.getConf.get("spark.app.ifUpdateBaseDataTime", "Y")

    if(whetherUpdateBaseDataTime != "Y" && whetherUpdateBaseDataTime != "N" ){
      logError("ifUpdateBaseDataTime类型错误错误, 期望值：Y, N ")
      return
    }


    // date related to haccg & pdsn
    val last7Hourtime = timeCalcWithFormatConvertSafe(curHourtime, "yyyyMMddHH", -7*60*60, "yyyyMMddHH")
    val dayidOfCurHourtime = curHourtime.substring(2, 8)
    val dayidOflast7Hourtime = last7Hourtime.substring(2, 8)
    val curHourid = curHourtime.substring(8, 10)
    val last7Hourid = last7Hourtime.substring(8, 10)

    val next2Hourtime = timeCalcWithFormatConvertSafe(curHourtime, "yyyyMMddHH", 2*60*60, "yyyyMMddHH")
    val next2Hourid = next2Hourtime.substring(8, 10)
    val dayidOfNext2Hourtime = next2Hourtime.substring(2, 8)
    ///////////////////////////////////////////////////////////////////////////////////////
    //  pdsn清单
    //
    ///////////////////////////////////////////////////////////////////////////////////////
    var pdsnMdn =
      s"""
         |select t.mdn, t.account_session_id, t.acct_status_type
         |from ${pdsnTable} t
         |where t.d='${dayidOflast7Hourtime}'
         |      and t.h>='${last7Hourid}' and t.h<'${curHourid}'
         |      and t.source_ip_address='0.0.0.0'
       """.stripMargin

    if(dayidOfCurHourtime>dayidOflast7Hourtime){
      pdsnMdn =
        s"""
           |select t.mdn, t.account_session_id, t.acct_status_type
           |from ${pdsnTable} t
           |where t.d='${dayidOflast7Hourtime}'
           |      and t.h>='${last7Hourid}'
           |      and t.source_ip_address='0.0.0.0'
           |union all
           |select t.mdn, t.account_session_id, t.acct_status_type
           |from ${pdsnTable} t
           |where t.d='${dayidOfCurHourtime}'
           |      and t.h<'${curHourid}'
           |      and t.source_ip_address='0.0.0.0'
       """.stripMargin
    }

    val tmpPdsnTable = "tmpPdsnTable_" + curHourtime
    sqlContext.sql(pdsnMdn).registerTempTable(tmpPdsnTable)
    sqlContext.cacheTable(tmpPdsnTable)


    ///////////////////////////////////////////////////////////////////////////////////////
    //  haccg清单
    //
    ///////////////////////////////////////////////////////////////////////////////////////

    var haccgMdn =
      s"""
         |select t.mdn, t.account_session_id, t.acct_status_type
         |from ${haccgTable} t
         |where t.d='${dayidOflast7Hourtime}'
         |      and t.h>='${last7Hourid}' and t.h<'${curHourid}'
         |      and t.source_ip_address='0.0.0.0'
       """.stripMargin

    if(dayidOfCurHourtime>dayidOflast7Hourtime){
      haccgMdn =
        s"""
           |select t.mdn, t.account_session_id, t.acct_status_type
           |from ${haccgTable} t
           |where t.d='${dayidOflast7Hourtime}'
           |      and t.h>='${last7Hourid}'
           |union all
           |select t.mdn, t.account_session_id, t.acct_status_type
           |from ${haccgTable} t
           |where t.d='${dayidOfCurHourtime}'
           |      and t.h<'${curHourid}'
       """.stripMargin
    }

    val tmpHaccgTable = "tmpHaccgTable_" + curHourtime
    sqlContext.sql(haccgMdn).registerTempTable(tmpHaccgTable)
    sqlContext.cacheTable(tmpHaccgTable)

    ///////////////////////////////////////////////////////////////////////////////////////
    //  pgw清单
    //
    ///////////////////////////////////////////////////////////////////////////////////////
    var pgwMdn =
      s"""
         |select l_timeoffirstusage, mdn
         |from ${pgwTable}
         |where d='${dayidOfCurHourtime}' and h>=${curHourid}  and h<${next2Hourid}
       """.stripMargin

    if(dayidOfNext2Hourtime > dayidOfCurHourtime){
      pgwMdn =
        s"""
           |select l_timeoffirstusage, mdn
           |from ${pgwTable}
           |where d='${dayidOfCurHourtime}' and h>=${curHourid}
           |union all
           |select l_timeoffirstusage, mdn
           |from ${pgwTable}
           |where d='${dayidOfNext2Hourtime}' and h<${next2Hourid}
       """.stripMargin
    }

    val tmpPgwTable = "tmpPgwTable_" + curHourtime
    sqlContext.sql(pgwMdn).registerTempTable(tmpPgwTable)
    sqlContext.cacheTable(tmpPgwTable)


    ///////////////////////////////////////////////////////////////////////////////////////
    //   统计在线清单
    //
    ///////////////////////////////////////////////////////////////////////////////////////
    val pdsnOnlineMdn =
      s"""
         |select r.mdn from
         |(
         |    select t1.mdn, t2.mdn as mdn2 from
         |        (select mdn, account_session_id from ${tmpPdsnTable} l1 where l1.acct_status_type<>'2') t1
         |    left join
         |        (select mdn, account_session_id from ${tmpPdsnTable} l2 where l2.acct_status_type='2' ) t2
         |    on(t1.mdn=t2.mdn and t1.account_session_id=t2.account_session_id)
         |) r
         |where r.mdn2 is null
       """.stripMargin

    val haccgOnlineMdn =
      s"""
         |select r.mdn from
         |(
         |    select t1.mdn, t2.mdn as mdn2 from
         |        (select mdn, account_session_id from ${tmpHaccgTable} l1 where l1.acct_status_type<>'2') t1
         |    left join
         |        (select mdn, account_session_id from ${tmpHaccgTable} l2 where l2.acct_status_type='2' ) t2
         |    on(t1.mdn=t2.mdn and t1.account_session_id=t2.account_session_id)
         |) r
         |where r.mdn2 is null
       """.stripMargin

    // 统计时间点的时间转换
    val timeOfFirstUsageStr = timeCalcWithFormatConvertSafe(curHourtime, "yyyyMMddHH",0, "yyyy-MM-dd HH:mm:ss")
    val pgwOnlineMdn =
      s"""
         |select mdn from ${tmpPgwTable} where l_timeoffirstusage < '${timeOfFirstUsageStr}
       """.stripMargin

    
  }
}
