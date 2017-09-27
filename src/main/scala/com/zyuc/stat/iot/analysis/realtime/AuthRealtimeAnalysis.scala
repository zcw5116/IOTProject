package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.HbaseDataUtil
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils, MathUtil}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * 认证日志实时分析
  *
  * @author zhoucw
  * @version 1.0
  *
  */
object AuthRealtimeAnalysis extends Logging{



  /**
    * 主函数
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    // 获取参数
    val appName = sc.getConf.get("spark.app.name","name_201708010040") // name_201708010040
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo") //
    val userAndDomainTable = sc.getConf.get("spark.app.table.userAndDomainTable", "iot_basic_user_and_domain")
    val companyAndDomainTable = sc.getConf.get("spark.app.table.companyAndDomainTable", "iot_basic_company_and_domain")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid")

    val auth3gTable = sc.getConf.get("spark.app.table.auth3gTable", "iot_userauth_3gaaa")
    val auth4gTable = sc.getConf.get("spark.app.table.auth4gTable", "iot_userauth_4gaaa")
    val authVPDNTable = sc.getConf.get("spark.app.table.authVPDNTable", "iot_userauth_vpdn")
    val alarmHtablePre = sc.getConf.get("spark.app.htable.alarmTablePre", "analyze_summ_tab_")
    val resultHtablePre = sc.getConf.get("spark.app.htable.resultHtablePre", "analyze_summ_rst_everycycle_")
    val resultDayHtable = sc.getConf.get("spark.app.htable.resultDayHtable", "analyze_summ_rst_everyday")
    val analyzeBPHtable = sc.getConf.get("spark.app.htable.analyzeBPHtable", "analyze_bp_tab")


    // 实时分析类型： 0-后续会离线重跑数据, 2-后续不会离线重跑数据
    val progRunType = sc.getConf.get("spark.app.progRunType", "0")

    if(progRunType!="0" && progRunType!="1" ) {
      logError("param progRunType invalid, expect:0|1")
      return
    }

    //  dataTime-当前数据时间  nextDataTime-下一个时刻数据的时间
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val nextDataTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 5*60, "yyyyMMddHHmm")
    // 转换成hive表中的时间格式
    val startTimeStr = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5*60, "yyyy-MM-dd HH:mm:ss")
    val endTimeStr = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 0, "yyyy-MM-dd HH:mm:ss")
    // 转换成hive表中的分区字段值
    val startTime =  DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5*60, "yyyyMMddHHmm")
    val partitionD = startTime.substring(0, 8)
    val partitionH = startTime.substring(8, 10)

    /////////////////////////////////////////////////////////////////////////////////////////
    //  Hbase 相关的表
    //  表不存在， 就创建
    /////////////////////////////////////////////////////////////////////////////////////////
    //  curAlarmHtable-当前时刻的预警表,  nextAlarmHtable-下一时刻的预警表,
    val curAlarmHtable = alarmHtablePre + dataTime.substring(0,8)
    val nextAlarmHtable = alarmHtablePre + nextDataTime.substring(0,8)
    val alarmFamilies = new Array[String](1)
    alarmFamilies(0) = "s"
    // 创建表, 如果表存在， 自动忽略
    HbaseUtils.createIfNotExists(curAlarmHtable,alarmFamilies)
    HbaseUtils.createIfNotExists(nextAlarmHtable,alarmFamilies)

    //  curResultHtable-当前时刻的结果表,  nextResultHtable-下一时刻的结果表
    val curResultHtable = resultHtablePre + dataTime.substring(0,8)
    val nextResultHtable = resultHtablePre + nextDataTime.substring(0,8)
    val resultFamilies = new Array[String](1)
    resultFamilies(0) = "s"
    HbaseUtils.createIfNotExists(curResultHtable, resultFamilies)
    HbaseUtils.createIfNotExists(nextResultHtable, resultFamilies)

    // resultDayHtable
    val resultDayFamilies = new Array[String](1)
    resultDayFamilies(0) = "s"
    HbaseUtils.createIfNotExists(resultDayHtable, resultDayFamilies)

    // analyzeBPHtable
    val analyzeBPFamilies = new Array[String](1)
    analyzeBPFamilies(0) = "bp"
    HbaseUtils.createIfNotExists(analyzeBPHtable, analyzeBPFamilies)


    ////////////////////////////////////////////////////////////////
    //   cache table
    ///////////////////////////////////////////////////////////////
    val userInfoTableCached = "userInfoTableCached"
    sqlContext.sql(s"cache table ${userInfoTableCached} as select mdn, imsicdma, companycode, isvpdn, isdirect from $userInfoTable where d=$userTableDataDayid")

    val userAndDomainTableCached = "userAndDomainTableCached"
    sqlContext.sql(s"cache table ${userAndDomainTableCached} as select mdn, companycode, isvpdn, vpdndomain from $userAndDomainTable where d=$userTableDataDayid")

    // 关联3g的mdn, domain,  基站
    val mdnSql =
      s"""
         |select '3g' type, u.mdn,
         |       regexp_extract(a.nai_sercode, '^.+@(.*vpdn.*)', 1) as vpdndomain,
         |       a.auth_result, a.nasportid,
         |       case when a.auth_result = 0 then 's' else 'f' end as result
         |from  ${userInfoTableCached} u, ${auth3gTable} a
         |where a.dayid = '${partitionD}'  and a.hourid = '${partitionH}'
         |      and u.imsicdma = a.imsicdma
         |      and a.auth_time >= '${startTimeStr}' and a.auth_time < '${endTimeStr}'
         |union all
         |select '4g' type, a.mdn,
         |       regexp_extract(a.nai_sercode, '^.+@(.*vpdn.*)', 1) as  vpdndomain,
         |       a.auth_result, a.nasportid,
         |       case when a.auth_result = 0 then 's' else 'f' end as result
         |from ${auth4gTable} a
         |where a.dayid = '${partitionD}'  and a.hourid = '${partitionH}'
         |      and a.auth_time >= '${startTimeStr}' and a.auth_time < '${endTimeStr}'
         |union all
         |select 'vpdn' type, a.mdn,
         |       regexp_extract(a.nai_sercode, '^.+@(.*vpdn.*)', 1) as vpdndomain,
         |       a.auth_result, "-1" as nasportid,
         |       case when a.auth_result = 1 then 's' else 'f' end as result
         |from ${authVPDNTable} a
         |where a.dayid = '${partitionD}'  and a.hourid = '${partitionH}'
         |      and a.auth_time >= '${startTimeStr}' and a.auth_time < '${endTimeStr}'
       """.stripMargin
    val mdnTable = "mdnTable_" + startTime
    sqlContext.sql(mdnSql).registerTempTable(mdnTable)


    // 统计普通业务/定向业务/总的业务数据
    // busiType： 业务类型（D-定向， P-普通， E-其他
    // type:  网络类型
    // req_cnt: 请求数
    // req_s_cnt: 请求成功数
    // req_card_cnt: 请求卡数
    // req_card_s_cnt: 请求成功卡数
    // req_card_f_cnt: 请求失败卡数
    // (case when isdirect=1 then 'D' when isblock!='x' then 'P' else 'E' end) busiType
    val statSQL =
      s"""
         |select companycode, busiType, type,
         |       sum(reqcnt) as req_cnt,
         |       sum(reqsucccnt) as req_s_cnt,
         |       count(distinct mdn) as req_card_cnt,
         |       sum(case when result='s' then 1 else 0 end) as req_card_s_cnt,
         |       sum(case when result='f' then 1 else 0 end) as req_card_f_cnt,
         |       GROUPING__ID
         |from
         |(
         |    select u.companycode,(case when isdirect=1 then 'D' else 'P' end) busiType, t.type, t.mdn, t.result,
         |           count(*) as reqcnt,
         |           sum(case when result='s' then 1 else 0 end) reqsucccnt
         |    from ${mdnTable} t, ${userInfoTableCached} u
         |    where t.mdn = u.mdn
         |    group by u.companycode, (case when isdirect=1 then 'D' else 'P' end) , t.type, t.mdn, t.result
         |) m
         |group by companycode, busiType, type
         |GROUPING SETS (companycode,(companycode, type),(companycode, busiType),(companycode, busiType, type))
         |ORDER BY GROUPING__ID
       """.stripMargin

    val statDF = sqlContext.sql(statSQL).coalesce(1)

    /*
      scala>  statDF.filter("companycode='P100002192'").show
      +-----------+--------+----+-------+---------+------------+--------------+--------------+------------+
      |companycode|busiType|type|req_cnt|req_s_cnt|req_card_cnt|req_card_s_cnt|req_card_f_cnt|grouping__id|
      +-----------+--------+----+-------+---------+------------+--------------+--------------+------------+
      | P100002192|    null|null|   8398|     8397|        3773|          3772|             1|           1|
      | P100002192|       D|null|    335|      334|         163|           162|             1|           3|
      | P100002192|       P|null|   8063|     8063|        3610|          3610|             0|           3|
      | P100002192|    null|  3g|   8398|     8397|        3773|          3772|             1|           5|
      | P100002192|       D|  3g|    335|      334|         163|           162|             1|           7|
      | P100002192|       P|  3g|   8063|     8063|        3610|          3610|             0|           7|
      +-----------+--------+----+-------+---------+------------+--------------+--------------+------------+
      */



    val startResultRDD = statDF.rdd.filter(x=>x(1)!="E").map(x=>{
      val busiType =  if(null == x(1)) "-1" else x(1).toString
      val netType = if(null == x(2)) "-1" else x(2).toString
      val netFlag = if(netType=="3g") "3" else if(netType=="4g") "4" else if(netType=="vpdn") "v" else "t"
      val servType = if(busiType == "D") "D" else if(busiType == "P") "P" else "-1"
      val companyCode = x(0)
      val reqCnt = x(3).toString
      val reqSuccCnt = x(4).toString
      val reqCardCnt = x(5).toString
      val reqCardSuccCnt = x(6).toString
      val reqCardFailedCnt = x(7).toString


      val curAlarmRowkey = progRunType + "_" + dataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + "-1"
      val curAlarmPut = new Put(Bytes.toBytes(curAlarmRowkey))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      val nextAlarmRowkey = progRunType + "_" + nextDataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + "-1"
      val nexAlarmtPut = new Put(Bytes.toBytes(nextAlarmRowkey))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rn"), Bytes.toBytes(x(3).toString))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_sn"), Bytes.toBytes(x(4).toString))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rcn"), Bytes.toBytes(x(5).toString))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_scn"), Bytes.toBytes(x(6).toString))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_fcn"), Bytes.toBytes(x(7).toString))

      val curResKey = companyCode +"_" + servType + "_" + "-1" + "_" + dataTime.substring(8,12)
      val curResPut = new Put(Bytes.toBytes(curResKey))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      val nextResKey = companyCode +"_" + servType + "_" + "-1" + "_" + nextDataTime.substring(8,12)
      val nextResPut = new Put(Bytes.toBytes(nextResKey))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      val dayResKey = dataTime.substring(2,8) + "_" + companyCode + "_" + servType + "_" + "-1"
      val dayResPut = new Put(Bytes.toBytes(dayResKey))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      ((new ImmutableBytesWritable, curAlarmPut), (new ImmutableBytesWritable, nexAlarmtPut), (new ImmutableBytesWritable, curResPut), (new ImmutableBytesWritable, nextResPut), (new ImmutableBytesWritable, dayResPut))
    })


    HbaseDataUtil.saveRddToHbase(curAlarmHtable, startResultRDD.map(x=>x._1))
    HbaseDataUtil.saveRddToHbase(nextAlarmHtable, startResultRDD.map(x=>x._2))
    HbaseDataUtil.saveRddToHbase(curResultHtable, startResultRDD.map(x=>x._3))
    HbaseDataUtil.saveRddToHbase(nextResultHtable, startResultRDD.map(x=>x._4))
    HbaseDataUtil.saveRddToHbase(resultDayHtable, startResultRDD.map(x=>x._5))


    // 统计VPDN业务数据
    val vpdnStatSQL =
      s"""
         |select companycode, vpdndomain, type as type,
         |       sum(d_request_cnt) as d_request_cnt,
         |       sum(d_request_s_cnt) as d_request_s_cnt,
         |       count(*) as t_request_card_cnt,
         |       sum(case when m.result='s' then 1 else 0 end) t_request_card_s_cnt,
         |       sum(case when m.result='f' then 1 else 0 end) t_request_card_f_cnt,
         |       GROUPING__ID
         |from
         |(
         |    select t.mdn, t.type, t.vpdndomain as tvpdndomain, t.result,
         |           count(*) as d_request_cnt,
         |           sum(case when t.result='s' then 1 else 0 end) as d_request_s_cnt
         |    from ${mdnTable} t
         |    group by t.mdn, t.type, t.vpdndomain, t.result
         |) m, ${userAndDomainTableCached} u
         |where m.mdn = u.mdn and m.tvpdndomain = u.vpdndomain and u.isvpdn=1
         |group by companycode, vpdndomain, type
         |GROUPING SETS (companycode,(companycode, type),(companycode, vpdndomain),(companycode, vpdndomain, type))
       """.stripMargin
    val vpdnStatDF = sqlContext.sql(vpdnStatSQL).coalesce(1)

    /*
         scala>  vpdnStatDF.filter("companycode='P100002368'").show
      +-----------+---------------+----+-------------+---------------+------------------+--------------------+--------------------+------------+
      |companycode|     vpdndomain|type|d_request_cnt|d_request_s_cnt|t_request_card_cnt|t_request_card_s_cnt|t_request_card_f_cnt|GROUPING__ID|
      +-----------+---------------+----+-------------+---------------+------------------+--------------------+--------------------+------------+
      | P100002368|           null|null|          398|            392|               328|                 324|                   4|           1|
      | P100002368|           null|  4g|          205|            205|               163|                 163|                   0|           5|
      | P100002368|fsgdjcb.vpdn.gd|null|          398|            392|               328|                 324|                   4|           3|
      | P100002368|fsgdjcb.vpdn.gd|  4g|          205|            205|               163|                 163|                   0|           7|
      | P100002368|           null|  3g|          193|            187|               165|                 161|                   4|           5|
      | P100002368|fsgdjcb.vpdn.gd|  3g|          193|            187|               165|                 161|                   4|           7|
      +-----------+---------------+----+-------------+---------------+------------------+--------------------+--------------------+------------+
    */

    val vpdnStatRDD = vpdnStatDF.rdd.map(x=>{

      val vpdnDomain = if(null == x(1)) "-1" else x(1)
      val netType = if(null == x(2)) "-1" else x(2)
      val netFlag = if(netType=="3g") "3" else if(netType=="4g") "4" else if(netType=="vpdn") "v" else "t"
      val servType = "C"
      val companyCode = x(0)
      val reqCnt = x(3).toString
      val reqSuccCnt = x(4).toString
      val reqCardCnt = x(5).toString
      val reqCardSuccCnt = x(6).toString
      val reqCardFailedCnt = x(7).toString

      val curAlarmKey = progRunType + "_" + dataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + vpdnDomain
      val curAlarmPut = new Put(Bytes.toBytes(curAlarmKey))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      val nextAlarmKey = progRunType + "_" + nextDataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + vpdnDomain
      val nexAlarmtPut = new Put(Bytes.toBytes(nextAlarmKey))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      val curResKey = companyCode +"_" + servType + "_" + vpdnDomain + "_" + dataTime.substring(8,12)
      val curResPut = new Put(Bytes.toBytes(curResKey))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      val nextResKey = companyCode +"_" + servType + "_" + vpdnDomain + "_" + nextDataTime.substring(8,12)
      val nexResPut = new Put(Bytes.toBytes(nextResKey))
      nexResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      nexResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      nexResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqSuccCnt, reqCnt)))
      nexResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      nexResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      nexResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      val dayResKey = dataTime.substring(2,8) + "_" + companyCode + "_" + servType + "_" + vpdnDomain
      val dayResPut = new Put(Bytes.toBytes(dayResKey))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rn"), Bytes.toBytes(reqCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_sn"), Bytes.toBytes(reqSuccCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rat"), Bytes.toBytes(MathUtil.divOpera(reqCnt, reqSuccCnt )))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rcn"), Bytes.toBytes(reqCardCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_scn"), Bytes.toBytes(reqCardSuccCnt))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(reqCardFailedCnt))

      ((new ImmutableBytesWritable, curAlarmPut), (new ImmutableBytesWritable, nexAlarmtPut), (new ImmutableBytesWritable, curResPut), (new ImmutableBytesWritable, nexResPut), (new ImmutableBytesWritable, dayResPut))
    })

    HbaseDataUtil.saveRddToHbase(curAlarmHtable, vpdnStatRDD.map(x=>x._1))
    HbaseDataUtil.saveRddToHbase(nextAlarmHtable, vpdnStatRDD.map(x=>x._2))
    HbaseDataUtil.saveRddToHbase(curResultHtable, vpdnStatRDD.map(x=>x._3))
    HbaseDataUtil.saveRddToHbase(nextResultHtable, vpdnStatRDD.map(x=>x._4))
    HbaseDataUtil.saveRddToHbase(resultDayHtable, vpdnStatRDD.map(x=>x._5))

    // 更新时间
    val analyzeColumn = if(progRunType == "0") "analyze_guess_bptime" else "analyze_real_bptime"
    HbaseUtils.upSertColumnByRowkey(analyzeBPHtable, "bp", "auth", analyzeColumn, dataTime)


  }

}
