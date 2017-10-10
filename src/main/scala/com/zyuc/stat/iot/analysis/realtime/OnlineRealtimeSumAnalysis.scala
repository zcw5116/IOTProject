package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.{AuthHtableConverter, HbaseDataUtil}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by zhoucw on 17-10-8.
  */
object OnlineRealtimeSumAnalysis extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    // 获取参数
    val appName = sc.getConf.get("spark.app.name","name_201710080040") // name_201708010040
    val userInfoTable = sc.getConf.get("spark.app.table.userInfoTable", "iot_basic_userinfo") //
    val userAndDomainTable = sc.getConf.get("spark.app.table.userAndDomainTable", "iot_basic_user_and_domain")
    val companyAndDomainTable = sc.getConf.get("spark.app.table.companyAndDomainTable", "iot_basic_company_and_domain")
    val userTableDataDayid = sc.getConf.get("spark.app.table.userTableDataDayid", "20170922")

    val auth3gTable = sc.getConf.get("spark.app.table.3gRadiusTable", "iot_radius_ha")
    val auth4gTable = sc.getConf.get("spark.app.table.4gRadiusTable", "pgwradius_out")
    val alarmHtablePre = sc.getConf.get("spark.app.htable.alarmTablePre", "analyze_summ_tab_")
    val resultHtablePre = sc.getConf.get("spark.app.htable.resultHtablePre", "analyze_summ_rst_everycycle_")
    val resultDayHtable = sc.getConf.get("spark.app.htable.resultDayHtable", "analyze_summ_rst_everyday")
    val analyzeBPHtable = sc.getConf.get("spark.app.htable.analyzeBPHtable", "analyze_bp_tab")

    // 实时分析类型： 0-后续会离线重跑数据, 2-后续不会离线重跑数据
    val progRunType = sc.getConf.get("spark.app.progRunType", "0")
    // 距离当前历史同期的天数
    val hisDayNumStr = sc.getConf.get("spark.app.hisDayNums", "7")

    if(progRunType!="0" && progRunType!="1" ) {
      logError("param progRunType invalid, expect:0|1")
      return
    }
    var hisDayNum:Int = 0
    try {
      hisDayNum = hisDayNumStr.toInt
    }catch {
      case e:Exception => {
        logError("TypeConvert Failed. hisDayNumStr [" + hisDayNumStr + " cannot convert to Int ] ")
        return
      }
    }
    //  dataTime-当前数据时间  nextDataTime-下一个时刻数据的时间
    val dataTime = appName.substring(appName.lastIndexOf("_") + 1)
    val nextDataTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 5*60, "yyyyMMddHHmm")
    // 转换成hive表中的时间格式
    val startTimeStr = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -5*60, "yyyyMMddHHmmss")
    val endTimeStr = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 0, "yyyyMMddHHmmss")
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
    val resultFamilies = new Array[String](2)
    resultFamilies(0) = "s"
    resultFamilies(1) = "e"
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


    val onlineBaseTable = "iot_useronline_basedata"
    // val baseDataHourid = HbaseUtils.getCloumnValueByRowkey("iot_dynamic_info","rowkey001","onlinebase","baseHourid") // 从habase里面获取
    val baseDataHourid = "2017100913"
    val baseTablePartition = baseDataHourid.substring(2)

    val beginDay = baseDataHourid.substring(0, 8)
    val endDay = dataTime.substring(0, 8)

    val startM5 = baseDataHourid.substring(8, 10)+"00"

    val endM5 = dataTime.substring(8, 12)

    // 获取间隔的天数
    val intervalDay = DateUtils.timeInterval(beginDay, endDay, "yyyyMMdd") / (24 * 60 * 60)


    var hbaseDF: DataFrame = null
    for (i <- 0 to intervalDay.toInt) {
      val dayid = DateUtils.timeCalcWithFormatConvertSafe(beginDay, "yyyyMMdd", i * 24 * 60 * 60, "yyyyMMdd")
      if (i == 0) {
        hbaseDF = AuthHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + dayid).filter(s"time > ${startM5}")
      } else if(i < intervalDay) {
        val tmpDF = AuthHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + dayid)
        hbaseDF = hbaseDF.unionAll(tmpDF)
      }

      if (i == intervalDay && intervalDay > 0) {
        val tmpDF = AuthHtableConverter.convertToDF(sc, sqlContext, resultHtablePre + dayid).filter(s"time <= ${endM5}")
        hbaseDF = hbaseDF.unionAll(tmpDF)
      }else if(intervalDay == 0){
        hbaseDF = hbaseDF.filter(s"time <= ${endM5}")
      }

    }

    val radiusDataTable = "radiusDataTable_" + dataTime
    hbaseDF = hbaseDF.selectExpr("rowkey","compnyAndSerAndDomain",
      "nvl(o_c_3_li,0) as o_c_3_li", "nvl(o_c_3_lo,0) as o_c_3_lo", "nvl(o_c_4_li,0) as o_c_4_li",
      "nvl(o_c_4_lo,0) as o_c_4_lo", "nvl(o_c_t_li,0) as o_c_t_li", "nvl(o_c_t_lo,0) as o_c_t_lo")
    hbaseDF.registerTempTable(radiusDataTable)
    sqlContext.cacheTable(radiusDataTable)




    val statSQL =
      s"""select compnyAndSerAndDomain, type, sum(usercnt) as usercnt
         |from
         |(
         |    select compnyAndSerAndDomain, '3' as type, (o_c_3_li - o_c_3_lo) as usercnt
         |    from ${radiusDataTable}
         |    union all
         |    select compnyAndSerAndDomain, '4' as type, (o_c_4_li - o_c_4_lo) as usercnt
         |    from ${radiusDataTable}
         |    union all
         |    select compnyAndSerAndDomain, 't' as type, (o_c_t_li - o_c_t_lo) as usercnt
         |    from ${radiusDataTable}
         |    union all
         |    select  concat_ws('_', nvl(companycode,'-1'), nvl(servtype,'-1'), nvl(vpdndomain,'-1')) as compnyAndSerAndDomain,
         |            nvl(type, 't') as type, usercnt
         |    from ${onlineBaseTable} where d='${baseTablePartition}'
         |) m
         |group by compnyAndSerAndDomain, type
       """.stripMargin

    val resultDF = sqlContext.sql(statSQL)
    val resultRDD = resultDF.coalesce(1).rdd.map(x=>{
      val csd = if( null == x(0)) x(0).toString else "-1"
      val csdArr = csd.split("_")
      var companyCode = "-1"
      var servType = "-1"
      var domain = "-1"
      if(csdArr.length == 3){
        companyCode = csdArr(0)
        servType = csdArr(1)
        domain = csdArr(2)
      }

      val netFlag = x(1).toString
      val usercnt = x(2).toString

      val curAlarmRowkey = progRunType + "_" + dataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + domain
      val curAlarmPut = new Put(Bytes.toBytes(curAlarmRowkey))
      curAlarmPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_c_" + netFlag + "_on"), Bytes.toBytes(usercnt))

      val nextAlarmRowkey = progRunType + "_" + nextDataTime.substring(8,12) + "_" + companyCode + "_" + servType + "_" + domain
      val nexAlarmtPut = new Put(Bytes.toBytes(nextAlarmRowkey))
      nexAlarmtPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_p_" + netFlag + "_on"), Bytes.toBytes(usercnt))

      val curResKey = companyCode +"_" + servType + "_" + domain + "_" + dataTime.substring(8,12)
      val curResPut = new Put(Bytes.toBytes(curResKey))
      curResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_c_" + netFlag + "_on"), Bytes.toBytes(usercnt))

      val nextResKey = companyCode +"_" + servType + "_" + domain + "_" + nextDataTime.substring(8,12)
      val nextResPut = new Put(Bytes.toBytes(nextResKey))
      nextResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_p_" + netFlag + "_on"), Bytes.toBytes(usercnt))

      val dayResKey = dataTime.substring(2,8) + "_" + companyCode + "_" + servType + "_" + domain
      val dayResPut = new Put(Bytes.toBytes(dayResKey))
      dayResPut.addColumn(Bytes.toBytes("s"), Bytes.toBytes("o_c_" + netFlag + "_on"), Bytes.toBytes(usercnt))

      ((new ImmutableBytesWritable, curAlarmPut), (new ImmutableBytesWritable, nexAlarmtPut), (new ImmutableBytesWritable, curResPut), (new ImmutableBytesWritable, nextResPut), (new ImmutableBytesWritable, dayResPut))
    })


    HbaseDataUtil.saveRddToHbase(curAlarmHtable, resultRDD.map(x=>x._1))
    HbaseDataUtil.saveRddToHbase(nextAlarmHtable, resultRDD.map(x=>x._2))
    HbaseDataUtil.saveRddToHbase(curResultHtable, resultRDD.map(x=>x._3))
    HbaseDataUtil.saveRddToHbase(nextResultHtable, resultRDD.map(x=>x._4))
    HbaseDataUtil.saveRddToHbase(resultDayHtable, resultRDD.map(x=>x._5))

  }

}
