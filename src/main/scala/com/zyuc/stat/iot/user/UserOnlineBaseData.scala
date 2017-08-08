package com.zyuc.stat.iot.user

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils.timeCalcWithFormatConvertSafe
import com.zyuc.stat.utils.{DateUtils, FileUtils, HbaseUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by zhoucw on 17-7-9.
  */
object UserOnlineBaseData extends Logging{

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setAppName("UserOnlineBaseData").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val hivedb = ConfigProperties.IOT_HIVE_DATABASE
    sqlContext.sql("use " + hivedb)


    val curHourtime = sc.getConf.get("spark.app.hourid") // 2017072412
    val outputPath = sc.getConf.get("spark.app.outputPath") // "/hadoop/IOT/ANALY_PLATFORM/UserOnline/"
    var userTablePartitionID = DateUtils.timeCalcWithFormatConvertSafe(curHourtime.substring(0,8), "yyyyMMdd", -1*24*3600, "yyyyMMdd")
    userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID", userTablePartitionID)
    val userTable = sc.getConf.get("spark.app.userTable") //"iot_customer_userinfo"
    val pdsnTable = sc.getConf.get("spark.app.pdsnTable")
    val pgwTable = sc.getConf.get("spark.app.pgwTable")


    val last7Hourtime = timeCalcWithFormatConvertSafe(curHourtime, "yyyyMMddHH", -7*60*60, "yyyyMMddHH")
    val dayidOfCurHourtime = curHourtime.substring(2, 8)
    val dayidOflast7Hourtime = last7Hourtime.substring(2, 8)
    val curHourid = curHourtime.substring(8, 10)
    val last7Hourid = last7Hourtime.substring(8, 10)
    val curPartDayrid = dayidOfCurHourtime

    val userDF = sqlContext.table(userTable).filter("d=" + userTablePartitionID).
      selectExpr("mdn", "imsicdma", "custprovince", "case when length(vpdncompanycode)=0 then 'N999999999' else vpdncompanycode end  as vpdncompanycode")

    // 缓存用户的表
    val cachedUserinfoTable = "iot_user_basic_info_cached"
    userDF.cache().registerTempTable(cachedUserinfoTable)


    val cachedCompanyTable = "cachedCompany"
    sqlContext.sql(s"""CACHE TABLE ${cachedCompanyTable} as select distinct vpdncompanycode from ${cachedUserinfoTable}""")


    // 0点在线的用户
    var commonSql = ""
    if(dayidOfCurHourtime>dayidOflast7Hourtime){
      commonSql =
        s"""
           |select t.mdn, t.account_session_id, t.acct_status_type
           |from ${pdsnTable} t
           |where t.d='${dayidOflast7Hourtime}' and t.h>='${last7Hourid}'
           |union all
           |select t.mdn, t.account_session_id, t.acct_status_type
           |from ${pdsnTable} t
           |where t.d='${dayidOfCurHourtime}' and t.h<='${curHourid}'
         """.stripMargin
    }else{
      commonSql =
        s"""
           |select t.mdn, t.account_session_id, t.acct_status_type
           |from ${pdsnTable} t
           |where t.d='${dayidOflast7Hourtime}'
           |      and t.h>='${last7Hourid}' and t.h<='${curHourid}'
           |""".stripMargin
    }

    val g3resultsql =
      s""" select r.mdn from
         |(
         |    select t1.mdn, t2.mdn as mdn2 from
         |        (select mdn, account_session_id from ( ${commonSql} ) l1 where l1.acct_status_type<>'2') t1
         |    left join
         |        (select mdn, account_session_id from ( ${commonSql} ) l2 where l2.acct_status_type='2' ) t2
         |    on(t1.mdn=t2.mdn and t1.account_session_id=t2.account_session_id)
         |) r
         |where r.mdn2 is null
       """.stripMargin

 /*   val g3usersql =
      s"""select t1.mdn from
         |    (select t.mdn, t.account_session_id
         |     from iot_cdr_3gaaa_ticket t
         |     where t.acct_status_type<>'2' and t.dayid='${dayidOfLastHourtime}' and t.hourid='${lastHourid}'
         |     ) t1,
         |    (select t.mdn, t.account_session_id
         |    from iot_cdr_3gaaa_ticket t
         |    where t.acct_status_type='2' and t.dayid='${dayidOfCurHourtime}' and t.hourid='${curHourid}'
         |    ) t2
         |where t1.mdn=t2.mdn and t1.account_session_id=t2.account_session_id
         |
       """.stripMargin*/

    val g3tmpuser  = "g3tmpuser" + curHourtime
    sqlContext.sql(g3resultsql).registerTempTable(g3tmpuser)

    val g3onlinecomptable = "g3onlinecomp" + curHourtime

    val g3onlinecompsql =
      s"""CACHE TABLE ${g3onlinecomptable} as select u.vpdncompanycode,count(*) as g3cnt from ${g3tmpuser} t, ${cachedUserinfoTable} u
         |  where t.mdn=u.mdn group by u.vpdncompanycode
       """.stripMargin
    sqlContext.sql(g3onlinecompsql).coalesce(1)

    val timeOfFirstUsageStr = timeCalcWithFormatConvertSafe(curHourtime, "yyyyMMddHH",0, "yyyy-MM-dd HH:mm:ss")
    val pgwonlinecomptable = "pgwonlinecomp" + curHourtime
    val pgwcompsql =
     s"""CACHE TABLE ${pgwonlinecomptable} as select o.vpdncompanycode, count(*) as pgwcnt
        |from ( SELECT u.mdn, u.vpdncompanycode
        |       FROM ${cachedUserinfoTable} u LEFT SEMI JOIN ${pgwTable} t
        |       ON  (u.mdn = t.mdn and t.d='${dayidOfCurHourtime}' and t.l_timeoffirstusage < '${timeOfFirstUsageStr}' and t.h>=${curHourid})
        |     ) o
        |group by o.vpdncompanycode
      """.stripMargin
    sqlContext.sql(pgwcompsql).coalesce(1)

/*    val companyonlinesum =
      s"""select '${curHourtime}' as hourtime,c.vpdncompanycode, nvl(t1.g3cnt,0) as g3cnt, nvl(t2.pgwcnt,0) as pgwcnt
         |from ${cachedCompanyTable} c
         |left join ${g3onlinecomptable} t1 on(c.vpdncompanycode=t1.vpdncompanycode)
         |left join ${pgwonlinecomptable} t2 on(c.vpdncompanycode=t2.vpdncompanycode)
       """.stripMargin*/

    // 暂时将g3置为0
    val companyonlinesum =
      s"""select '${curPartDayrid}' as d, '${curHourid}' as h,c.vpdncompanycode, 0 as g3cnt, nvl(t2.pgwcnt,0) as pgwcnt
         |from ${cachedCompanyTable} c
         |left join ${g3onlinecomptable} t1 on(c.vpdncompanycode=t1.vpdncompanycode)
         |left join ${pgwonlinecomptable} t2 on(c.vpdncompanycode=t2.vpdncompanycode)
       """.stripMargin


    // 外部分区表
    // create table iot_external_useronline_base (vpdncompanycode string, g3cnt int, pgwcnt int) partitioned by (dayid string)
    //  row format delimited
    //  fields terminated by '\t'
    //  location 'hdfs://hadoop11:9000/dir2';

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)


    val partitions = "d,h"

    def getTemplate: String = {
      var template = ""
      val partitionArray = partitions.split(",")
      for (i <- 0 until partitionArray.length)
        template = template + "/" + partitionArray(i) + "=*"
      template // rename original dir
    }


    sqlContext.sql(companyonlinesum).repartition(1).write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath + s"temp/${curHourtime}")
    val outFiles = fileSystem.globStatus(new Path(outputPath + "temp/" + curHourtime + getTemplate + "/*.orc"))
    val filePartitions = new mutable.HashSet[String]
    for (i <- 0 until outFiles.length) {
      val nowPath = outFiles(i).getPath.toString
      filePartitions.+=(nowPath.substring(0, nowPath.lastIndexOf("/")).replace(outputPath + "temp/" + curHourtime, "").substring(1))
    }

    FileUtils.moveTempFiles(fileSystem, outputPath, curHourtime, getTemplate, filePartitions)


    //sqlContext.read.format("orc").load("/hadoop/IOT/ANALY_PLATFORM/UserOnline/20170709").registerTempTable("tttt")
    //sqlContext.sql("select vpdncompanycode, g3cnt, pgwcnt from tttt where vpdncompanycode='C000000517'").collect().foreach(println)
    val sql = s"alter table iot_useronline_base_nums add IF NOT EXISTS partition(d='$curPartDayrid', h='$curHourid')"
    logInfo("sql:" + sql)
    sqlContext.sql(sql)

    // 写入hbase表
    HbaseUtils.updateCloumnValueByRowkey("iot_dynamic_info","rowkey001","onlinebase","baseHourid", curHourtime) // 从habase里面获取


    sc.stop()

    }

}
