package com.zyuc.stat.iot.mme

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils.timeCalcWithFormatConvertSafe
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by slview on 17-7-9.
  */
object UserOnlineBaseData {

  def main(args: Array[String]): Unit = {
      if (args.length < 1) {
        System.err.println("Usage: <yyyyMMdd>")
        System.exit(1)
      }

      val dayid = args(0)
      val lastdayid = timeCalcWithFormatConvertSafe(dayid,"yyyyMMdd", -1*24*60*60,"yyyyMMdd")
      println(lastdayid)


    val sparkConf = new SparkConf().setAppName("MMEAnalysis").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)

    // 缓存用户的表
    val cachedUserinfoTable = "iot_user_basic_info_cached"
    sqlContext.sql(
      s"""
         |CACHE TABLE ${cachedUserinfoTable} as
         |select u.mdn,case when length(u.vpdncompanycode)=0 then 'N999999999' else u.vpdncompanycode end  as vpdncompanycode
         |from iot_user_basic_info u
       """.stripMargin).repartition(1)

    val cachedCompanyTable = "cachedCompany"
    sqlContext.sql(s"""CACHE TABLE ${cachedCompanyTable} as select distinct vpdncompanycode from ${cachedUserinfoTable}""")


    // 0点在线的用户
    val g3usersql =
      s"""select t1.mdn from
         |    (select t.mdn, t.account_session_id
         |     from iot_cdr_haccg_ticket t
         |     where t.acct_status_type<>'2' and t.dayid='${lastdayid}' and t.hourid=23
         |     ) t1,
         |    (select t.mdn, t.account_session_id
         |    from iot_cdr_haccg_ticket t
         |    where t.acct_status_type='2' and t.dayid='${dayid}' and t.hourid=00
         |    ) t2
         |where t1.mdn=t2.mdn and t1.account_session_id=t2.account_session_id
         |
       """.stripMargin

    val g3tmpuser  = "g3tmpuser" + dayid
    sqlContext.sql(g3usersql).registerTempTable(g3tmpuser)

    val g3onlinecomptable = "g3onlinecomp" + dayid

    val g3onlinecompsql =
      s"""CACHE TABLE ${g3onlinecomptable} as select u.vpdncompanycode,count(*) as g3cnt from ${g3tmpuser} t, ${cachedUserinfoTable} u
         |  where t.mdn=u.mdn group by u.vpdncompanycode
       """.stripMargin
    sqlContext.sql(g3onlinecompsql).coalesce(1)

    val daystr = timeCalcWithFormatConvertSafe(dayid, "yyyyMMdd",0, "yyyy-MM-dd HH:mm:ss")
    val pgwonlinecomptable = "pgwonlinecomp" + dayid
    val pgwcompsql =
     s"""CACHE TABLE ${pgwonlinecomptable} as select o.vpdncompanycode, count(*) as pgwcnt
        |from ( SELECT u.mdn, u.vpdncompanycode
        |       FROM iot_user_basic_info u LEFT SEMI JOIN iot_cdr_pgw_ticket t
        |       ON  (u.mdn = t.mdn and t.dayid='${dayid}' and t.l_timeoffirstusage < '${daystr}' and t.hourid<3)
        |     ) o
        |group by o.vpdncompanycode
      """.stripMargin
    sqlContext.sql(pgwcompsql).coalesce(1)

    val companyonlinesum =
      s"""select c.vpdncompanycode, nvl(t1.g3cnt,0) as g3cnt, nvl(t2.pgwcnt,0) as pgwcnt
         |from ${cachedCompanyTable} c
         |left join ${g3onlinecomptable} t1 on(c.vpdncompanycode=t1.vpdncompanycode)
         |left join ${pgwonlinecomptable} t2 on(c.vpdncompanycode=t2.vpdncompanycode)
       """.stripMargin


    // 外部分区表
    // create table iot_external_useronline_base (vpdncompanycode string, g3cnt int, pgwcnt int) partitioned by (dayid string)
    //  row format delimited
    //  fields terminated by '\t'
    //  location 'hdfs://hadoop11:9000/dir2';


    sqlContext.sql(companyonlinesum).coalesce(1).write.mode(SaveMode.Overwrite).format("orc").save(s"/hadoop/IOT/ANALY_PLATFORM/UserOnline/${dayid}")
    //sqlContext.read.format("orc").load("/hadoop/IOT/ANALY_PLATFORM/UserOnline/20170709").registerTempTable("tttt")
    //sqlContext.sql("select vpdncompanycode, g3cnt, pgwcnt from tttt where vpdncompanycode='C000000517'").collect().foreach(println)
    sc.stop()

    }

}
