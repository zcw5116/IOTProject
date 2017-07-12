package com.zyuc.stat.iot.user

import java.text.SimpleDateFormat
import java.util.Calendar

import com.zyuc.stat.utils.{CommonUtils, HbaseUtils}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Result, Scan, Connection, Put}
import org.apache.hadoop.hbase.filter.{RegexStringComparator, CompareFilter, RowFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.io.Source

/**
 * Created by cuihs on 2017/6/26.
 */

object IotUserLineDetail {
  def main(args: Array[String]) {
    val fileName = "/home/slview/test/wangpf/BPFile/IotUserLineDetail.BP"
    val (startTime, endTime, statisday, statishhmm, lastPeriodday, lastPeriodhhmm) = getStatisTime(fileName)
    // 创建表
    if (statishhmm == "0000")
      createTbaleIfexists(statisday)
    // 创建上下文
    val sparkConf = new SparkConf()
      .setAppName("IotUserLineDetail")

    val sc = new SparkContext(sparkConf)

    // 创建hiveContext
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use iot")
    hiveContext.sql("set hive.mapred.supports.subdirectories=true")
    hiveContext.sql("set mapreduce.input.fileinputformat.input.dir.recursive=true")
    hiveContext.sql("set mapred.max.split.size=256000000")
    hiveContext.sql("set mapred.min.split.size.per.node=128000000")
    hiveContext.sql("set mapred.min.split.size.per.rack=128000000")
    hiveContext.sql("set hive.hadoop.supports.splittable.combineinputformat=true")
    hiveContext.sql("set hive.exec.compress.output=true")
    hiveContext.sql("set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec")
    hiveContext.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")

    hiveContext.sql("set hive.merge.mapfiles=true")
    hiveContext.sql("set hive.merge.mapredfiles=true")
    hiveContext.sql("set hive.merge.size.per.task=64000000")
    hiveContext.sql("set hive.merge.smallfiles.avgsize=64000000")

    hiveContext.sql("set hive.groupby.skewindata=true")

    val sql =
      "select iubi.vpdncompanycode, po.TerminateCause, " +
        " count(case when po.NetType = '3G' and po.Status = 'Start' then 1 else null end), " +
        " count(case when po.NetType = '3G' and po.Status = 'Stop' then 1 else null end), " +
        " count(case when po.NetType = '4G' and po.Status = 'Start' then 1 else null end), " +
        " count(case when po.NetType = '4G' and po.Status = 'Stop' then 1 else null end) " +
        "from iot.iot_user_basic_info iubi " +
        "inner join iot.pgwradius_out po " +
        "on iubi.mdn = po.mdn " +
        s" and po.dayid = '${statisday}' " +
        s" and time >= '${startTime}' " +
        s" and time < '${endTime}' " +
        "group by iubi.vpdncompanycode, po.TerminateCause"

    println("sql = " + sql)

    val resault = hiveContext.sql(sql).collect()

    sc.stop()

    val companylineinfo = scala.collection.mutable.Map[(String, String), Long]()
    val companytermcase = scala.collection.mutable.Map[String, ArrayBuffer[Map[String, String]]]()
    // 获取公司列表
    val companylist = scala.collection.mutable.Map[String, Long]()
    for (x <- resault) {
      val company = if (x(0).toString.length == 0) "N999999999" else x(0).toString

      companylineinfo((company, "3gStart")) = getMapData((company, "3gStart"), companylineinfo, x(2).toString)
      companylineinfo((company, "3gStop")) = getMapData((company, "3gStop"), companylineinfo, x(3).toString)
      companylineinfo((company, "4gStart")) = getMapData((company, "4gStart"), companylineinfo, x(4).toString)
      companylineinfo((company, "4gStop")) = getMapData((company, "4gStop"), companylineinfo, x(5).toString)
      // 计算每个公司的正常下线用户数
      if (x(1).toString == "UserRequest" || x(1).toString == "") {
        companylineinfo((company, "err3gStop")) = getMapData((company, "err3gStop"), companylineinfo, x(3).toString)
        companylineinfo((company, "err4gStop")) = getMapData((company, "err4gStop"), companylineinfo, x(5).toString)
      } else {
        companylineinfo((company, "err3gStop")) = getMapData((company, "err3gStop"), companylineinfo, "0")
        companylineinfo((company, "err4gStop")) = getMapData((company, "err4gStop"), companylineinfo, "0")
      }
      // 获取每个原因下线的用户数
      val tmpmap1 = scala.collection.mutable.Map[String, String]()
      val tmpmap2 = scala.collection.mutable.Map[String, String]()

      val TerminateCause = if (x(1).toString == "") "null" else x(1).toString
      tmpmap1(TerminateCause + "_3g_cnt") = x(3).toString
      tmpmap2(TerminateCause + "_4g_cnt") = x(5).toString

      if (companytermcase.contains(company))
        companytermcase(company) ++= Array(tmpmap1, tmpmap2)
      else
        companytermcase(company) = ArrayBuffer(tmpmap1, tmpmap2)

      companylist(company) = 1L
    }

    val conn = HbaseUtils.getConnect("EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16", "2181")

    // 获取上一个周期的数据
    val lastPerioddData = RecordFilter(conn, "iot_detail_online_users_" + lastPeriodday, lastPeriodhhmm)

    val hbaseTbale = conn.getTable(TableName.valueOf("iot_detail_online_users_" + statisday))
    val hbaseTbale2 = conn.getTable(TableName.valueOf("analyze_rst_tab"))

    companylist.keys.foreach( companycode => {
      val company = if (companycode.length == 0) "N999999999" else companycode

      val put = new Put((company + "_" + statishhmm).getBytes)

      if (companylineinfo((company, "3gStart")) > 0)
        put.addColumn("lineinfo".getBytes, "3gStart".getBytes, companylineinfo((company, "3gStart")).toString.getBytes())
      if (companylineinfo((company, "3gStop")) > 0)
        put.addColumn("lineinfo".getBytes, "3gStop".getBytes, companylineinfo((company, "3gStop")).toString.getBytes())
      if (companylineinfo((company, "4gStart")) > 0)
        put.addColumn("lineinfo".getBytes, "4gStart".getBytes, companylineinfo((company, "4gStart")).toString.getBytes())
      if (companylineinfo((company, "4gStop")) > 0)
        put.addColumn("lineinfo".getBytes, "4gStop".getBytes, companylineinfo((company, "4gStop")).toString.getBytes())

      companytermcase(company).foreach( map => {
        map.keys.foreach( key => {
          if (map(key).toLong > 0)
            put.addColumn("termcase".getBytes, key.getBytes, map(key).getBytes())
        })
      })

      hbaseTbale.put(put)

      val put2 = new Put(company.toString.getBytes)
      put2.addColumn("alarmChk".getBytes, "logout_c_time".getBytes, (statisday + statishhmm).getBytes())
      put2.addColumn("alarmChk".getBytes, "totalLogout_c_3g_cnt".getBytes, companylineinfo((company, "3gStop")).toString.getBytes())
      put2.addColumn("alarmChk".getBytes, "totalLogout_p_3g_cnt".getBytes, getMapData1(company, lastPerioddData, 1).toString.getBytes())
      put2.addColumn("alarmChk".getBytes, "totalLogout_c_4g_cnt".getBytes, companylineinfo((company, "4gStop")).toString.getBytes())
      put2.addColumn("alarmChk".getBytes, "totalLogout_p_4g_cnt".getBytes, getMapData1(company, lastPerioddData, 3).toString.getBytes())

      put2.addColumn("alarmChk".getBytes, "normalLogout_c_3g_cnt".getBytes, companylineinfo((company, "err3gStop")).toString.getBytes())
      put2.addColumn("alarmChk".getBytes, "normalLogout_p_3g_cnt".getBytes, getMapData1(company, lastPerioddData, 2).toString.getBytes())
      put2.addColumn("alarmChk".getBytes, "normalLogout_c_4g_cnt".getBytes, companylineinfo((company, "err4gStop")).toString.getBytes())
      put2.addColumn("alarmChk".getBytes, "normalLogout_p_4g_cnt".getBytes, getMapData1(company, lastPerioddData, 4).toString.getBytes())

      hbaseTbale2.put(put2)
    })

    hbaseTbale.close()
    hbaseTbale2.close()
    conn.close()

    // 更新BPTime
    CommonUtils.updateBptime(fileName, endTime.substring(0, 12))
  }

  private def getStatisTime(fileName: String): (String, String, String, String, String, String) ={
    var startTime: String = null
    var endTime: String = null
    var statisday: String = null
    var statishhmm: String = null
    var lastPeriodday: String = null
    var lastPeriodhhmm: String = null
    // 从文件中获取参数
    val source = Source.fromFile(fileName)
    val time = source.getLines.next()
    source.close

    val m = "[0-9]{12}".r
    time match {
      case m() => {
        startTime = time + "00"
        statisday = startTime.substring(0, 8)
        statishhmm = startTime.substring(8, 12)
        val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        val inDate = sdf.parse(startTime)
        val calendar = Calendar.getInstance()
        calendar.setTime(inDate)
        calendar.add(Calendar.MINUTE, 5)
        endTime = sdf.format(calendar.getTime())

        calendar.setTime(inDate)
        calendar.add(Calendar.MINUTE, -5)
        lastPeriodday = sdf.format(calendar.getTime()).substring(0, 8)
        lastPeriodhhmm = sdf.format(calendar.getTime()).substring(8, 12)
      }
      case other => {
        println("param error!")
        System.exit(0)
      }
    }

    println(startTime + "," + endTime + "," + statisday)
    (startTime, endTime, statisday, statishhmm, lastPeriodday, lastPeriodhhmm)
  }

  private def createTbaleIfexists(statisDay: String): Unit = {
    val conn = HbaseUtils.getConnect("EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16", "2181")
    //Hbase表模式管理器
    val admin = conn.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf("iot_detail_online_users_" + statisDay)
    //如果需要创建表
    if (!admin.tableExists(tableName)) {
      //创建Hbase表模式
      val tableDescriptor = new HTableDescriptor(tableName)
      //创建列簇1
      tableDescriptor.addFamily(new HColumnDescriptor("lineinfo".getBytes()))
      //创建列簇2
      tableDescriptor.addFamily(new HColumnDescriptor("termcase".getBytes()))
      //创建表
      admin.createTable(tableDescriptor)
      println("create done.")
    }
    conn.close()
  }

  private def getMapData (key: (String, String), map: Map[(String, String), Long], value: String): Long = {
    if (map.contains(key))
      map(key) + value.toLong
    else
      value.toLong
  }

  private def getMapData1 (key: String, map: Map[String, (Long, Long, Long, Long)], value: Int): Long = {
    if (map.contains(key))
      value match {
        case 1 => map(key)._1
        case 2 => map(key)._2
        case 3 => map(key)._3
        case 4 => map(key)._4
        case other => 0L
      }
    else
      0L
  }

  // 扫描记录并对rowkey模糊匹配
  private def RecordFilter(connection: Connection, tablename: String, Hhmm: String): mutable.Map[String, (Long, Long, Long, Long)] ={
    val dataMap = scala.collection.mutable.Map[String, (Long, Long, Long, Long)]()

    val userTable = TableName.valueOf(tablename)
    val table = connection.getTable(userTable)
    val s = new Scan()
    val filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(("^.*" + Hhmm+ "$")))
    s.setFilter(filter)
    s.addColumn("lineinfo".getBytes(), "3gStop".getBytes())
    s.addColumn("lineinfo".getBytes(), "4gStop".getBytes())
    s.addColumn("termcase".getBytes(), "UserRequest_3g_cnt".getBytes())
    s.addColumn("termcase".getBytes(), "UserRequest_4g_cnt".getBytes())
    s.addColumn("termcase".getBytes(), "unknow_3g_cnt".getBytes())
    s.addColumn("termcase".getBytes(), "unknow_4g_cnt".getBytes())
    val scanner = table.getScanner(s)
    var result: Result = scanner.next()
    while(result != null) {
      val total3gStop =
        if (result.containsColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStop")))
          Bytes.toString(result.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("3gStop"))).toLong
        else
          0L
      val total4gStop =
        if (result.containsColumn(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStop")))
          Bytes.toString(result.getValue(Bytes.toBytes("lineinfo"), Bytes.toBytes("4gStop"))).toLong
        else
          0L
      val normal3gStop_UserRequest =
        if (result.containsColumn(Bytes.toBytes("termcase"), Bytes.toBytes("UserRequest_3g_cnt")))
          Bytes.toString(result.getValue(Bytes.toBytes("termcase"), Bytes.toBytes("UserRequest_3g_cnt"))).toLong
        else
          0L
      val normal3gStop_unknow =
        if (result.containsColumn(Bytes.toBytes("termcase"), Bytes.toBytes("unknow_3g_cnt")))
          Bytes.toString(result.getValue(Bytes.toBytes("termcase"), Bytes.toBytes("unknow_3g_cnt"))).toLong
        else
          0L
      val normal4gStop_UserRequest =
        if (result.containsColumn(Bytes.toBytes("termcase"), Bytes.toBytes("UserRequest_4g_cnt")))
          Bytes.toString(result.getValue(Bytes.toBytes("termcase"), Bytes.toBytes("UserRequest_4g_cnt"))).toLong
        else
          0L
      val normal4gStop_unknow =
        if (result.containsColumn(Bytes.toBytes("termcase"), Bytes.toBytes("unknow_4g_cnt")))
          Bytes.toString(result.getValue(Bytes.toBytes("termcase"), Bytes.toBytes("unknow_4g_cnt"))).toLong
        else
          0L

      dataMap(Bytes.toString(result.getRow).split("_")(0)) =
        (total3gStop, normal3gStop_UserRequest + normal3gStop_unknow,
          total4gStop, normal4gStop_UserRequest + normal4gStop_unknow)

      result = scanner.next()
    }
    table.close()
    scanner.close()

    dataMap
  }
}