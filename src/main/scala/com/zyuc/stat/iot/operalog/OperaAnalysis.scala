package com.zyuc.stat.iot.operalog

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

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
    sqlContext.sql("CACHE LAZY TABLE " + cachedUserinfoTable + "  as select u.mdn, " +
      "case when length(u.vpdncompanycode)=0 then 'N999999999' else u.vpdncompanycode end  as vpdncompanycode " +
      "from iot_user_basic_info u ").coalesce(1)

    val userTypeSql =
      s"""
         |select nvl(t1.mdn, t2.mdn) as mdn, if(t1.mdn is not null, 1, 0) as g23flag, if(t2.mdn is not null, 1, 0) as g4flag,
         |if(t1.mdn is not null and t2.mdn is not null ,1, 0) as g234flag, if(t1.mdn is not null, t1.opertype, t2.opertype) as opertype
         |from
         |    (
         |        select l.mdn, l.opertype from  iot_opera_log l
         |        where l.platform='HLR' and l.d = '${operaDay}' and length(mdn)>0 and l.opertype in('开户','销户') and l.oper_result='成功'
         |    ) t1
         |full outer join
         |    (
         |        select l.mdn, l.opertype from  iot_opera_log l
         |        where l.platform='HSS' and l.d = '${operaDay}' and length(mdn)>0 and l.opertype in('开户','销户') and l.oper_result='成功'
         |    ) t2
         |on(t1.mdn = t2.mdn)
       """.stripMargin

    val userTypeTable = "userTypeTable"
    sqlContext.sql(userTypeSql).registerTempTable(userTypeTable)

    val resultSql =
      s"""
         |select u.vpdncompanycode,
         |sum(case when opertype='开户' and g23flag=1 then 1 else 0 end) g23opensum,
         |sum(case when opertype='销户' and g23flag=1 then 1 else 0 end) g23closesum,
         |sum(case when opertype='开户' and g4flag=1 then 1 else 0 end) g4opensum,
         |sum(case when opertype='销户' and g23flag=1 then 1 else 0 end) g4closesum,
         |sum(case when opertype='开户' and g234flag=1 then 1 else 0 end) g234opensum,
         |sum(case when opertype='销户' and g234flag=1 then 1 else 0 end) g234closesum
         |from ${userTypeTable} t, ${cachedUserinfoTable} u where t.mdn = u.mdn
         |group by u.vpdncompanycode
       """.stripMargin

    val df = sqlContext.sql(resultSql)
    df.show()
  }

}
