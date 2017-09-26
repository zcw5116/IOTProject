package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.HbaseDataUtil
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
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

    // 获取参数
    val appName = sc.getConf.get("spark.app.name","name_201708010040") // name_201708010040
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo") //
    val userAndDomainTable = sc.getConf.get("spark.app.table.userAndDomainTable", "iot_basic_user_and_domain")
    val companyAndDomainTable = sc.getConf.get("spark.app.table.companyAndDomainTable", "iot_basic_company_and_domain")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid")

    val auth3gTable = sc.getConf.get("spark.app.table.auth3gTable", "iot_userauth_3gaaa")
    val auth4gTable = sc.getConf.get("spark.app.table.auth4gTable", "iot_userauth_4gaaa")
    val authVPDNTable = sc.getConf.get("spark.app.table.authVPDNTable", "iot_userauth_vpdn")

    // 实时分析类型： 0-后续会离线重跑数据, 2-后续不会离线重跑数据
    val progRunType = sc.getConf.get("spark.app.progRunType", "0")

    if(progRunType!="0" && progRunType!="1" ) {
      logError("param progRunType invalid, expect:0|1")
      return
    }

    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val startTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5*60, "yyyyMMddHHmm")
    val endTime = dataTime
    val startTimeStr = DateUtils.timeCalcWithFormatConvertSafe(startTime, "yyyyMMddHHmm", 0, "yyyy-MM-dd HH:mm:ss")
    val endTimeStr = DateUtils.timeCalcWithFormatConvertSafe(endTime, "yyyyMMddHHmm", 0, "yyyy-MM-dd HH:mm:ss")
    val partitionD = startTime.substring(0, 8)
    val partitionH = startTime.substring(8, 10)

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

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


    //  统计普通业务/定向业务/总的业务数据
    // d_request_cnt: 定向业务请求数, d_request_f_cnt: 定向业务请求失败数
    // c_request_cnt: 普通业务请求数, c_request_f_cnt: 普通业务请求失败数
    // t_request_cnt: 总请求数,      t_request_f_cnt: 总请求失败数
    // t_request_card_cnt: 请求卡数, t_request_card_s_cnt: 请求成功卡数 , t_request_card_f_cnt: 请求失败卡数

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



    val nextEndTime = DateUtils.timeCalcWithFormatConvertSafe(endTime, "yyyyMMddHHmm", 5*60, "yyyyMMddHHmm")
    val startResultRDD = statDF.rdd.filter(x=>x(1)!="E").map(x=>{
      val busiType =  if(null == x(1)) "-1" else x(1).toString
      val netType = if(null == x(2)) "-1" else x(2).toString
      val netFlag = if(netType=="3g") "3" else if(netType=="4g") "4" else if(netType=="vpdn") "v" else "t"
      val busiFlag = if(busiType == "D") "D" else if(busiType == "P") "P" else "-1"
      val curRowkey = progRunType + "_" + endTime.substring(8,12) + "_" + x(0) + "_" + busiFlag + "_" + "-1"
      val nextRowkey = progRunType + "_" + nextEndTime.substring(8,12) + "_" + x(0) + "_" + busiFlag + "_" + "-1"
      val curPut = new Put(Bytes.toBytes(curRowkey))
      val nextPut = new Put(Bytes.toBytes(nextRowkey))
      curPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rn"), Bytes.toBytes(x(3).toString))
      curPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_sn"), Bytes.toBytes(x(4).toString))
      curPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rcn"), Bytes.toBytes(x(5).toString))
      curPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_scn"), Bytes.toBytes(x(6).toString))
      curPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(x(7).toString))
      nextPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rn"), Bytes.toBytes(x(3).toString))
      nextPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_sn"), Bytes.toBytes(x(4).toString))
      nextPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rcn"), Bytes.toBytes(x(5).toString))
      nextPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_scn"), Bytes.toBytes(x(6).toString))
      nextPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(x(7).toString))
      ((new ImmutableBytesWritable, curPut), (new ImmutableBytesWritable, nextPut))
    })

    val alarmHtable = "analyze_summ_tab_" + endTime.substring(0,8)
    val families = new Array[String](1)
    families(0) = "s"
    HbaseDataUtil.saveRddToHbase(alarmHtable, families, startResultRDD.map(x=>x._1))
    HbaseDataUtil.saveRddToHbase(alarmHtable, families, startResultRDD.map(x=>x._2))


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
      val busiFlag = "C"
      val curRowkey = progRunType + "_" + endTime.substring(8,12) + "_" + x(0) + "_" + busiFlag + "_" + vpdnDomain
      val nextRowkey = progRunType + "_" + nextEndTime.substring(8,12) + "_" + x(0) + "_" + busiFlag + "_" + vpdnDomain
      val curPut = new Put(Bytes.toBytes(curRowkey))
      val nextPut = new Put(Bytes.toBytes(nextRowkey))
      curPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rn"), Bytes.toBytes(x(3).toString))
      curPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_sn"), Bytes.toBytes(x(4).toString))
      curPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_rcn"), Bytes.toBytes(x(5).toString))
      curPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_scn"), Bytes.toBytes(x(6).toString))
      curPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(x(7).toString))
      nextPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rn"), Bytes.toBytes(x(3).toString))
      nextPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_sn"), Bytes.toBytes(x(4).toString))
      nextPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_rcn"), Bytes.toBytes(x(5).toString))
      nextPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_p_" + netFlag + "_scn"), Bytes.toBytes(x(6).toString))
      nextPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("a_c_" + netFlag + "_fcn"), Bytes.toBytes(x(7).toString))

      ((new ImmutableBytesWritable, curPut), (new ImmutableBytesWritable, nextPut))
    })

    HbaseDataUtil.saveRddToHbase(alarmHtable, families, vpdnStatRDD.map(x=>x._1))
    HbaseDataUtil.saveRddToHbase(alarmHtable, families, vpdnStatRDD.map(x=>x._2))



  }

}
