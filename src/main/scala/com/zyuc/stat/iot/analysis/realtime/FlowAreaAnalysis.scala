package com.zyuc.stat.iot.analysis.realtime

import com.zyuc.stat.iot.analysis.util.{AreaHtableConverter, HbaseDataUtil, MMEBaseStationHtableConverter}
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * 认证日志实时分析
  *
  * @author zhoucw
  * @version 1.0
  *
  */
object FlowAreaAnalysis extends Logging {

  /**
    * 主函数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    // 获取参数
    val appName = sc.getConf.get("spark.app.name", "name_201710261340") // name_201708010040

    val resultBSHtable = sc.getConf.get("spark.app.htable.resultBSHtable", "analyze_summ_rst_bs")
    val areaRankHtable = sc.getConf.get("spark.app.htable.areaRankHtable", "analyze_summ_rst_area_rank")
    val bs4gTable = sc.getConf.get("spark.app.table.bs4gTable", "iot_basestation_4g")
    val bs3gTable = sc.getConf.get("spark.app.table.bs4gTable", "iot_basestation_3g")

    val analyzeBPHtable = sc.getConf.get("spark.app.htable.analyzeBPHtable", "analyze_bp_tab")


    // 实时分析类型： 0-后续会离线重跑数据, 2-后续不会离线重跑数据
    val progRunType = sc.getConf.get("spark.app.progRunType", "0")
    // 距离当前历史同期的天数
    val hisDayNumStr = sc.getConf.get("spark.app.hisDayNums", "7")

    if (progRunType != "0" && progRunType != "1") {
      logError("param progRunType invalid, expect:0|1")
      return
    }

    // resultDayHtable
    val families = new Array[String](1)
    families(0) = "r"
    HbaseUtils.createIfNotExists(areaRankHtable, families)


    def areaAnalysis(dataTime: String, analyType: String): Unit = {
      val dataDayid = dataTime.substring(0, 8)
      val oneHoursAgo = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", -60 * 60, "yyyyMMddHHmm")
      if (analyType != "h" && analyType != "d") {
        logError("analyType para required: d or h")
        return
      }
      val rowStart = if (analyType == "h") oneHoursAgo + "_P000000000" else dataDayid
      val rowEnd = dataTime + "_P999999999"

      val hbaseDF = AreaHtableConverter.convertToDF(sc, sqlContext, resultBSHtable, (rowStart, rowEnd))
      val tmpHtable = "tmpHtable_" + dataTime
      hbaseDF.registerTempTable(tmpHtable)

      val bsG3SQL =
        s"""
           |select h.cAndSAndD, '3g' nettype,
           |       nvl(b.provcode, '0') provid, nvl(b.provname, '0') provname,
           |       nvl(b.citycode, '0') cityid, nvl(b.cityname, '0') cityname,
           |       nvl(h.enbid, '0') enbid,
           |       nvl(concat_ws('_', h.enbid, b.cityname), '0')  zhlabel,
           |       h.ma_sn, h.ma_rn, h.ma_rat,
           |       h.a_sn, h.a_rn, h.a_rat,
           |       h.f_d, h.f_u
           |from ${tmpHtable} h left join ${bs3gTable} b on(substr(h.enbid, 1, 4) = b.bsidpre)
           |where h.nettype='3g'
           |union all
           |select h.cAndSAndD, '4g' nettype,
           |       nvl(b.provid, '0') provid, nvl(b.provname, '0') provname,
           |       nvl(b.cityid, '0') cityid, nvl(b.cityname, '0') cityname,
           |       nvl(b.enbid, '0') enbid,
           |       nvl(b.zhlabel,'0') zhlabel,
           |       h.ma_sn, h.ma_rn, h.ma_rat,
           |       h.a_sn, h.a_rn, h.a_rat,
           |       h.f_d, h.f_u
           |from   ${tmpHtable} h left join ${bs4gTable} b on(h.enbid = b.enbid)
           |where  h.nettype='4g'
       """.stripMargin
      val bsG3Table = "bsG3Table_" + dataTime
      sqlContext.sql(bsG3SQL).cache().registerTempTable(bsG3Table)


      /*     val bsG3StatSQL =
        s"""
           |select cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, zhlabel,
           |       ma_sn, ma_rn, ma_rat,
           |       a_sn, a_rn, a_rat,
           |       f_d, f_u, f_t,
           |       row_number() over(partition by cAndSAndD, provid, nettype order by ma_rn desc) as ma_n_p_rank,
           |       row_number() over(partition by cAndSAndD, cityid, nettype order by ma_rn desc) as ma_n_c_rank,
           |       row_number() over(partition by cAndSAndD, enbid, nettype order by ma_rn desc) as ma_n_b_rank,
           |       row_number() over(partition by cAndSAndD, provid order by ma_rn desc) as ma_t_p_rank,
           |       row_number() over(partition by cAndSAndD, cityid order by ma_rn desc) as ma_t_c_rank,
           |       row_number() over(partition by cAndSAndD, enbid order by ma_rn desc) as ma_t_b_rank,
           |       row_number() over(partition by cAndSAndD, provid, nettype order by a_rn desc) as a_p_rank,
           |       row_number() over(partition by cAndSAndD, cityid, nettype order by a_rn desc) as a_c_rank,
           |       row_number() over(partition by cAndSAndD, enbid, nettype order by a_rn desc) as a_b_rank,
           |       row_number() over(partition by cAndSAndD, provid, nettype order by f_t desc) as f_p_rank,
           |       row_number() over(partition by cAndSAndD, cityid, nettype order by f_t desc) as f_c_rank,
           |       row_number() over(partition by cAndSAndD, enbid, nettype order by f_t desc) as f_b_rank
           |from
           |(
           |select cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, zhlabel,
           |       sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,  nvl(sum(ma_sn)/sum(ma_rn)) ma_rat,
           |       sum(a_sn) a_sn, sum(a_rn) a_rn,  nvl(sum(a_sn)/sum(a_rn)) a_rat,
           |       sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t
           | from ${bsG3Table}
           | group by cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, zhlabel
           | grouping sets((cAndSAndD, provid, provname), (cAndSAndD, cityid, cityname), (cAndSAndD, enbid, zhlabel),
           | (cAndSAndD, provid, provname, nettype), (cAndSAndD, cityid, cityname, nettype), (cAndSAndD, enbid, zhlabel, nettype))
           | ) t
         """.stripMargin*/


      val bsG3StatSQL =
        s"""
           |select cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, zhlabel,
           |       sum(ma_sn) ma_sn, sum(ma_rn) ma_rn,  nvl(sum(ma_sn)/sum(ma_rn)) ma_rat,
           |       sum(a_sn) a_sn, sum(a_rn) a_rn,  nvl(sum(a_sn)/sum(a_rn)) a_rat,
           |       sum(f_d) f_d, sum(f_u) f_u, (sum(f_d) + sum(f_u)) f_t
           | from ${bsG3Table}
           | group by cAndSAndD, nettype, provid, provname, cityid, cityname, enbid, zhlabel
           | grouping sets((cAndSAndD, provid, provname), (cAndSAndD, cityid, cityname), (cAndSAndD, enbid, zhlabel),
           | (cAndSAndD, provid, provname, nettype), (cAndSAndD, cityid, cityname, nettype), (cAndSAndD, enbid, zhlabel, nettype))
         """.stripMargin


      val bsG4SQL =
        s"""
           |select h.cAndSAndD, '4g' nettype,
           |       nvl(b.provid, '0') provid, nvl(b.provname, '0') provname,
           |       nvl(b.cityid, '0') cityid, nvl(b.cityname, '0') cityname,
           |       nvl(b.enbid, '0') enbid,
           |       nvl(b.zhlabel,'0') zhlabel,
           |       h.ma_sn, h.ma_rn, h.ma_rat,
           |       h.a_sn, h.a_rn, h.a_rat,
           |       h.f_d, h.f_u
           |from   ${tmpHtable} h left join ${bs4gTable} b on(h.enbid = b.enbid)
           |where  h.nettype='4g'
       """.stripMargin
      val bsG4Table = "bsG4Table_" + dataTime
      sqlContext.sql(bsG4SQL).cache().registerTempTable(bsG4Table)


      // 更新时间, 断点时间比数据时间多1分钟
      val updateTime = DateUtils.timeCalcWithFormatConvertSafe(dataTime, "yyyyMMddHHmm", 1 * 60, "yyyyMMddHHmm")
      val analyzeColumn = if (progRunType == "0") "analyze_guess_bptime" else "analyze_real_bptime"
      HbaseUtils.upSertColumnByRowkey(analyzeBPHtable, "bp", "mme", analyzeColumn, updateTime)


    }

  }
}
