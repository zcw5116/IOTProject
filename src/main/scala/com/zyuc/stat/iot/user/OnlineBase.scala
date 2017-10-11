package com.zyuc.stat.iot.user

import com.zyuc.stat.iot.user.UserOnlineBaseData.{logError, logInfo}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils.timeCalcWithFormatConvertSafe
import com.zyuc.stat.utils.HbaseUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * desc: 统计某个时间点的在线用户数
  * @author zhoucw
  * @version 1.0
  */
object OnlineBase extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)

    val appName = sc.getConf.get("spark.app.name","OnlineBase_2017100712") //
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid", "20170922")
    val pdsnTable = sc.getConf.get("spark.app.table.pdsnTable", "iot_cdr_data_pdsn")
    val pgwTable = sc.getConf.get("spark.app.table.pgwTable", "iot_cdr_data_pgw")
    val haccgTable = sc.getConf.get("spark.app.table.haccgTable", "iot_cdr_data_haccg")
    val basenumTable = sc.getConf.get("spark.app.table.basenumTable", "iot_useronline_basedata")
    val ifUpdateBaseDataTime = sc.getConf.get("spark.app.ifUpdateBaseDataTime", "Y")
    val outputPath = sc.getConf.get("spark.app.outputPath", "/hadoop/IOT/ANALY_PLATFORM/UserOnline/") //

    if(ifUpdateBaseDataTime != "Y" && ifUpdateBaseDataTime != "N" ){
      logError("ifUpdateBaseDataTime类型错误错误, 期望值：Y, N ")
      return
    }

    val vpnToApnMapFile = sc.getConf.get("spark.app.vpnToApnMapFile", "/hadoop/IOT/ANALY_PLATFORM/BasicData/VpdnToApn/vpdntoapn.txt")
    import  sqlContext.implicits._
    val vpnToApnDF  = sqlContext.read.format("text").load(vpnToApnMapFile).map(x=>x.getString(0).split(",")).map(x=>(x(0),x(1))).toDF("vpdndomain","apn")
    val vpdnAndApnTable = "vpdnAndApnTable"
    vpnToApnDF.registerTempTable(vpdnAndApnTable)


    // date related to haccg & pdsn
    val curHourtime = appName.substring(appName.lastIndexOf("_")+1)
    val last7Hourtime = timeCalcWithFormatConvertSafe(curHourtime, "yyyyMMddHH", -7*60*60, "yyyyMMddHH")
    val dayidOfCurHourtime = curHourtime.substring(2, 8)
    val dayidOflast7Hourtime = last7Hourtime.substring(2, 8)
    val curHourid = curHourtime.substring(8, 10)
    val last7Hourid = last7Hourtime.substring(8, 10)

    val next2Hourtime = timeCalcWithFormatConvertSafe(curHourtime, "yyyyMMddHH", 2*60*60, "yyyyMMddHH")
    val next2Hourid = next2Hourtime.substring(8, 10)
    val dayidOfNext2Hourtime = next2Hourtime.substring(2, 8)

    ////////////////////////////////////////////////////////////////
    //   cache table
    ///////////////////////////////////////////////////////////////
    val userInfoTableCached = "userInfoTableCached"
    sqlContext.sql(s"cache table ${userInfoTableCached} as select mdn, companycode, vpdndomain, isvpdn, isdirect, iscommon from $userInfoTable where d=$userTableDataDayid")




    ///////////////////////////////////////////////////////////////////////////////////////
    //  pdsn清单
    //
    ///////////////////////////////////////////////////////////////////////////////////////
    var pdsnMdn =
      s"""
         |select t.mdn, regexp_replace(t.nai,'.*@','') as vpdndomain, t.account_session_id, t.acct_status_type
         |from ${pdsnTable} t
         |where t.d='${dayidOflast7Hourtime}'
         |      and t.h>='${last7Hourid}' and t.h<'${curHourid}'
         |      and t.source_ip_address='0.0.0.0'
       """.stripMargin

    if(dayidOfCurHourtime>dayidOflast7Hourtime){
      pdsnMdn =
        s"""
           |select t.mdn, regexp_replace(t.nai,'.*@','') as vpdndomain, t.account_session_id, t.acct_status_type
           |from ${pdsnTable} t
           |where t.d='${dayidOflast7Hourtime}'
           |      and t.h>='${last7Hourid}'
           |      and t.source_ip_address='0.0.0.0'
           |union all
           |select t.mdn, regexp_replace(t.nai,'.*@','') as vpdndomain, t.account_session_id, t.acct_status_type
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

    var pgwMdn = s"""
                    |select l_timeoffirstusage, mdn, accesspointnameni as apn
                    |from ${pgwTable}
                    |where d='${dayidOfCurHourtime}' and h>=${curHourid} and h<${next2Hourid}
       """.stripMargin

    if(dayidOfNext2Hourtime > dayidOfCurHourtime){
      pgwMdn =
        s"""
           |select l_timeoffirstusage, mdn, accesspointnameni as apn
           |from ${pgwTable}
           |where d='${dayidOfCurHourtime}' and h>=${curHourid}
           |union all
           |select l_timeoffirstusage, mdn, accesspointnameni as apn
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
    val pdsnOnlineMDNTmp =
      s"""
         |select distinct r.mdn, r.vpdndomain from
         |(
         |    select t1.mdn, t1.vpdndomain, t2.mdn as mdn2 from
         |        (select mdn, vpdndomain, account_session_id from ${tmpPdsnTable} l1 where l1.acct_status_type<>'2') t1
         |    left join
         |        (select mdn, account_session_id from ${tmpPdsnTable} l2 where l2.acct_status_type='2' ) t2
         |    on(t1.mdn=t2.mdn and t1.account_session_id=t2.account_session_id)
         |) r
         |where r.mdn2 is null
       """.stripMargin
    val pdsnMDNTmpTable = "pdsnMDNTmpTable_" + curHourtime
    sqlContext.sql(pdsnOnlineMDNTmp).cache().registerTempTable(pdsnMDNTmpTable)


    val haccgOnlineMdnTmp =
      s"""
         |select distinct r.mdn from
         |(
         |    select t1.mdn, t2.mdn as mdn2 from
         |        (select mdn, account_session_id from ${tmpHaccgTable} l1 where l1.acct_status_type<>'2') t1
         |    left join
         |        (select mdn, account_session_id from ${tmpHaccgTable} l2 where l2.acct_status_type='2' ) t2
         |    on(t1.mdn=t2.mdn and t1.account_session_id=t2.account_session_id)
         |) r
         |where r.mdn2 is null
       """.stripMargin
    val haccgMDNTmpTable = "haccgMDNTmpTable_" + curHourtime
    sqlContext.sql(haccgOnlineMdnTmp).registerTempTable(haccgMDNTmpTable)

    // 统计时间点的时间转换
    val timeOfFirstUsageStr = timeCalcWithFormatConvertSafe(curHourtime, "yyyyMMddHH",0, "yyyy-MM-dd HH:mm:ss")

    val pgwOnlineMdnTmp =
      s"""
         |select distinct mdn,apn from ${tmpPgwTable} where l_timeoffirstusage < '${timeOfFirstUsageStr}'
       """.stripMargin
    val pgwMDNTmpTable = "pgwMDNTmpTable_" + curHourtime
    sqlContext.sql(pgwOnlineMdnTmp).registerTempTable(pgwMDNTmpTable)



    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    //  tongji
    //
    ///////////////////////////////////////////////////////////////////////////////////////////////////////

    val statSQL =
      s"""
         |select companycode, mdn, servtype, c.vpdndomain, type
         |from
         |(
         |    select   p.mdn, u.companycode, 'C' as servtype,
         |           (case when array_contains(split(u.vpdndomain,','), p.vpdndomain) then p.vpdndomain else u.vpdndomain end) as vpdndomains,
         |           '3g' type
         |    from ${pdsnMDNTmpTable} p, ${userInfoTableCached} u
         |    where p.mdn = u.mdn and u.isvpdn='1'
         |) m lateral view explode(split(m.vpdndomains,',')) c as vpdndomain
         |union all
         |select u.companycode, a.mdn,
         |       (case when u.isdirect=1 then 'D' else 'P' end) as servtype,
         |       '-1'  as vpdndomain, '3g' type
         |from ${haccgMDNTmpTable} a, ${userInfoTableCached} u
         |where u.mdn = a.mdn
         |union all
         |select companycode,mdn,
         |(case when u.isdirect='1' then 'D' when u.isvpdn=1 and array_contains(split(u.vpdndomain,','), d.vpdndomain) then 'C' else 'P' end) as servtype,
         |(case when u.isdirect!='1' and array_contains(split(u.vpdndomain,','), d.vpdndomain) then d.vpdndomain else '-1' end)  as vpdndomain,
         |'4g' type
         |from
         |(
         |    select t.mdn, p.apn, t.companycode, t.vpdndomain, t.isdirect, t.isvpdn
         |    from  ${pgwMDNTmpTable} p, ${userInfoTableCached} t
         |    where p.mdn = t.mdn
         |) u left join ${vpdnAndApnTable} d
         |on(u.apn = d.apn and array_contains(split(u.vpdndomain,','),d.vpdndomain) )
       """.stripMargin

    val  statDF = sqlContext.sql(statSQL)
    val statDFTable = "statDFTable_" + curHourtime
    statDF.registerTempTable(statDFTable)


    val statResultDF = sqlContext.sql(
      s"""
         |select companycode, servtype, vpdndomain, type,
         |       count(*) as usercnt
         |from ${statDFTable}  m
         |group by companycode, servtype, vpdndomain, type
         |GROUPING SETS (companycode,(companycode, type),(companycode, servtype),(companycode, servtype, vpdndomain), (companycode, servtype, type), (companycode, servtype, vpdndomain, type))
       """.stripMargin)


    val partitionD = curHourtime.substring(2,10)
    statResultDF.filter("vpdndomain is null or vpdndomain!='-1'").repartition(1).write.format("orc").mode(SaveMode.Overwrite).save(outputPath + "data/d=" + partitionD)

    val sql = s"alter table ${basenumTable} add IF NOT EXISTS partition(d='$partitionD')"
    logInfo("sql:" + sql)
    sqlContext.sql(sql)

    // 写入hbase表
    if(ifUpdateBaseDataTime == "Y"){
      logInfo("write whetherUpdateBaseDataTime to Hbase Table. ")
      HbaseUtils.updateCloumnValueByRowkey("iot_dynamic_data","rowkey001","onlinebase","baseHourid", curHourtime) // 从habase里面获取
    }

    ///////////////////////////////////////////////////////////////////////////////////////
    //   根据业务统计在线用户数
    //
    ///////////////////////////////////////////////////////////////////////////////////////

    // vpdn汇总/vpdn按照域名汇总

    /*





    val vpdnMDN =
      s"""
         |select distinct mdn, type from
         |(
         |    select mdn, '3g' as type from ${pdsnMDNTable}
         |    union all
         |    select mdn, '4g' as type from ${pgwMDNTable}
         |) t
       """.stripMargin
    val vpdnTable = "vpdnTable_" + curHourtime
    sqlContext.sql(vpdnMDN).registerTempTable(vpdnTable)
    sqlContext.cacheTable(vpdnTable)

    val vpdnStatSQL =
      s"""
         |select u.companycode, 'C' as servtype, "-1" as vpdndomain, c.type, count(*) usercnt
         |from ${userInfoTableCached} u, ${vpdnTable} c
         |where c.mdn = u.mdn and u.isvpdn='1'
         |group by companycode, type
         |GROUPING SETS(companycode, (companycode, type))
         |union all
         |select t.companycode, 'C' as servtype, t.vpdndomain, t.type, count(*) usercnt
         |from(
         |select companycode, c.vpdndomain, type
         |from
         |( select u.companycode, vpdndomain, c.type
         |from ${userInfoTableCached} u, ${vpdnTable} c
         |where c.mdn = u.mdn and u.isvpdn='1'
         |) m lateral view explode(split(m.vpdndomain,',')) c as vpdndomain
         |) t
         |group by companycode, vpdndomain, type
         |GROUPING SETS((companycode, vpdndomain), (companycode, vpdndomain, type))
       """.stripMargin


    // 普通企业/定向
    val haccgAndPgwMDN =
      s"""
         |select mdn, '3g' type from ${haccgMDNTable}
         |union all
         |select mdn, '4g' type from ${pgwMDNTable}
       """.stripMargin
    val haccgAndPgwTable = "vpdnTable_" + curHourtime
    sqlContext.sql(haccgAndPgwMDN).registerTempTable(haccgAndPgwTable)
    sqlContext.cacheTable(haccgAndPgwTable)

    val CommonAndDirectStatSQL =
      s"""select companycode, servtype, "-1" as vpdndomain, type, count(*) as usercnt, GROUPING__ID
         |from
         |(
         |    select u.companycode,
         |           (case when u.isdirect='1' then 'D' when u.iscommon='1' then 'P' else '-1' end) as servtype,
         |           c.type
         |    from ${userInfoTableCached} u, ${haccgAndPgwTable} c
         |    where c.mdn = u.mdn
         |) m
         |group by companycode, servtype, type
         |GROUPING SETS((companycode, servtype), (companycode, servtype, type))
       """.stripMargin

    // 整个企业

*/


  }
}
