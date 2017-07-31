package com.zyuc.stat.iot.operalog

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}
/**
  * Created by zhoucw on 17-7-25.
  */
object OperaAnalysis {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("OperalogAnalysis")//.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val operaDay = sc.getConf.get("spark.app.operaDay")

    val cachedUserinfoTable = "iot_user_basic_info_cached"
    sqlContext.sql(
      s"""CACHE LAZY TABLE ${cachedUserinfoTable} as
         |select u.mdn, u.custprovince,
         |       case when length(u.vpdncompanycode)=0 then 'N999999999' else u.vpdncompanycode end  as vpdncompanycode
         |from iot_customer_userinfo u where u.d='${operaDay}'
       """.stripMargin)


    val cachedOperaTable = s"iot_opera_log_cached_$operaDay"
    sqlContext.sql(
      s"""CACHE TABLE ${cachedOperaTable} as
         |select l.mdn, l.platform, l.opertype
         |from iot_opera_log l
         |where l.opertype in('install','remove') and l.oper_result='成功' and length(mdn)>0 and  l.d = '${operaDay}'
       """.stripMargin)

    val openSql =
      s"""
         |select nvl(l1.mdn, l2.mdn) mdn,
         |case when l2.mdn is null then '2/3G' when l1.mdn is null then '4G' else '2/3/4G' end as nettype,
         |1 as opennum, 0 as closenum
         |from
         |    (select * from ${cachedOperaTable}  where opertype='install' and platform='HLR') l1
         |    full outer join
         |    (select * from ${cachedOperaTable}  where opertype='install' and platform='HSS') l2
         |    on(l1.mdn = l2.mdn)
       """.stripMargin

    val openDF = sqlContext.sql(openSql)

    val closeSql =
      s"""
         |select nvl(l1.mdn, l2.mdn) mdn,
         |case when l2.mdn is null then '2/3G' when l1.mdn is null then '4G' else '2/3/4G' end as nettype,
         |0 as opennum, 1 as closenum
         |from
         |    (select * from ${cachedOperaTable}  where opertype='remove' and platform='HLR') l1
         |    full outer join
         |    (select * from ${cachedOperaTable}  where opertype='remove' and platform='HSS') l2
         |    on(l1.mdn = l2.mdn)
       """.stripMargin

    val closeDF = sqlContext.sql(closeSql)

    val allDF = openDF.unionAll(closeDF)

    val operaTable = "operaTable" + operaDay
    allDF.registerTempTable(operaTable)


    val resultSql =
      s"""
         |select u.custprovince, u.vpdncompanycode, nettype,
         |sum(opennum) opensum,
         |sum(closenum) closesum
         |from ${operaTable} t, ${cachedUserinfoTable} u where t.mdn = u.mdn
         |group by u.custprovince, u.vpdncompanycode, nettype
       """.stripMargin

    val df = sqlContext.sql(resultSql)
    df.show()

    df.coalesce(1).write.mode(SaveMode.Overwrite).format("json").save("/tmp/json")
  }

}
