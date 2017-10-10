package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by dell on 2017/9/14.
  */
object CommonMultiAnalybak extends Logging {
  def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf()
      val sc = new SparkContext(sparkConf)
      val sqlContext = new HiveContext(sc)
      sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
      val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

      // 入参处理
      val sumitemtype = sc.getConf.get("spark.app.item.type") //"authlog_3gaaa;authlog_4gaaa,authlog_pdsn"
      val begintime = sc.getConf.get("spark.app.begintime") //20170826010000 yyyymmddhhmiss
      val endtime = sc.getConf.get("spark.app.endtime")//20170826020000 yyyymmddhhmiss
      val interval = sc.getConf.get("spark.app.interval")//15
      val cyctype = sc.getConf.get("spark.app.cyctype")// min/h/d/w/m
      val userTable = sc.getConf.get("spark.app.user.table") // "iot_customer_userinfo"
      val userTablePartitionDayid = sc.getConf.get("spark.app.user.userTablePatitionDayid") //  "20170801"
      val inputPath = sc.getConf.get("spark.app.inputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/authlog/15min/;
      val outputPath = sc.getConf.get("spark.app.outputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/authlog/15min/;

      //val localOutputPath =  sc.getConf.get("spark.app.jsonpath") // /slview/test/limm/multiAna/authlog/15min/json/;

      logInfo("##########--sumitemtype: " + sumitemtype)
      logInfo("##########--begintime:   " + begintime)
      logInfo("##########--endtime:     " + endtime)
      logInfo("##########--interval:    " + interval)
      logInfo("##########--cyctype:     " + cyctype)


      val ItmeName = sumitemtype.substring(0, sumitemtype.lastIndexOf("_") ) //"authlog"
      val subItmeName = sumitemtype.substring(sumitemtype.lastIndexOf("_") + 1) //"4g"

      // 判断统计起始和截止时间
      var beginStr: String = null //>=201709150100
      var endStr: String = null //<201709150200
      var intervalbegin: Int = 0
      var intervalend: Int = 0
      var timeformat: String = null
      var weekinfo:Map[String,String] = Map()
      var monthinfo:Map[String,String] = Map()

      // 按照小时和分钟汇总时，需要向前推一个周期
      if (cyctype == "min") {
        timeformat = "yyyyMMddHHmm"
        intervalbegin = interval.toInt * 60 * (-1)
        intervalend = interval.toInt * 60 * (-1) +60*60
        beginStr = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0, 12), "yyyyMMddHHmm", intervalbegin, "yyyyMMddHHmmss")
        endStr = DateUtils.timeCalcWithFormatConvertSafe(endtime.substring(0, 12), "yyyyMMddHHmm", intervalend, "yyyyMMddHHmmss")
      } else if (cyctype == "h") {
        timeformat = "yyyyMMddHH"
        intervalbegin = interval.toInt * 60 * (-1)
        intervalend = interval.toInt * 60 * (-1)
        beginStr = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0, 10), "yyyyMMddHH", intervalbegin, "yyyyMMddHHmmss")
        endStr = DateUtils.timeCalcWithFormatConvertSafe(endtime.substring(0, 10), "yyyyMMddHH", intervalend, "yyyyMMddHHmmss")
      } else if (cyctype == "d") {
        timeformat = "yyyyMMdd"
        beginStr = begintime.substring(0, 8)
        endStr = endtime.substring(0, 8)
      } else if (cyctype == "w") {
        timeformat = "yyyyMMdd"
        beginStr = begintime.substring(0, 8)
        endStr   = endtime.substring(0, 8)
        weekinfo = DateUtils.getWeeks(beginStr, timeformat)
        beginStr = weekinfo("firstday")
        endStr   = weekinfo("lastday")
      }else if(cyctype == "m"){
        timeformat = "yyyyMM"
        beginStr = begintime.substring(0, 6)
        endStr   = endtime.substring(0, 6)
        monthinfo = DateUtils.getmonths(beginStr, timeformat)
        beginStr = monthinfo("firstday")
        monthinfo = DateUtils.getmonths(endStr, timeformat)
        endStr   = monthinfo("lastday")
      }
      logInfo("##########--beginStr:    " + beginStr)
      logInfo("##########--endStr:      " + endStr)

      var filterDF:DataFrame = null

      // 按照小时汇总
      if(cyctype == "min" ||cyctype == "h"){
        filterDF = LoadfileByPartitinH(sqlContext,beginStr,endStr,inputPath)
        //时间过滤
        if(ItmeName == "authlog" && cyctype == "min"){
          filterDF.filter("authtime>="+beginStr).filter("authtime<"+endStr).
            withColumn("datetime",from_unixtime( unix_timestamp(col("authtime"),"yyyyMMddHHmmss") - 15*60*60,"yyyyMMddHHmm"))
        }else if(subItmeName == "mme" && cyctype == "min"){
          filterDF.filter("starttime>="+beginStr).filter("starttime<"+endStr).
            withColumn("starttime",from_unixtime( unix_timestamp(col("starttime"),"yyyyMMddHHmmss") - 15*60*60,"yyyyMMddHHmm"))
        }
      }





      //按照天汇总
      if(cyctype == "d" ||cyctype == "w" || cyctype == "m"){

      }



  }

  def  LoadfileByPartitinH(sqlContext:SQLContext,beginStr:String,endStr:String,inputPath:String): DataFrame = {
    var filterDF: DataFrame = null
    var sourceDF: DataFrame = null
    var inputLocation: String = null

    if (beginStr == null || endStr == null || inputPath == null) {
      return filterDF
    }
    //根据分区加载数据
    if (beginStr.substring(0, 10) == endStr.substring(0, 10)) {
      inputLocation = inputPath + "/d=" + beginStr.substring(2, 8) + "/h=" + endStr.substring(8, 10)

    } else {
      val intervalHourNums = (DateUtils.timeInterval(beginStr.substring(0, 10), endStr.substring(0, 10), "yyyyMMddHH")) / 3600
      for (i <- 0 to intervalHourNums.toInt) {
        val curtime = DateUtils.timeCalcWithFormatConvertSafe(beginStr.substring(0, 10), "yyyyMMddHH", i * 60 * 60, "yyyyMMddHH")
        inputLocation = inputPath + "/d=" + curtime.substring(2, 8) + "/h=" + curtime.substring(8, 10)
        sourceDF = sqlContext.read.format("orc").load(inputLocation)
        // 分区合并
        if (filterDF == null && sourceDF == null) {
          logInfo("##########--ERROR:filterDF is null and sourceDF is null !!")

        } else if (filterDF == null && sourceDF != null) {
          filterDF = sourceDF
        }
        else if (filterDF != null && sourceDF == null) {
          logInfo("##########--ERROR:filterDF is not null but sourceDF is null !!")
        }
        else if (filterDF != null && sourceDF != null) {
          filterDF = filterDF.unionAll(sourceDF)
        }
      }

    }
    //返回值
    filterDF
  }

    def LoadfileByPartitinD(sqlContext: SQLContext, beginStr: String, endStr: String, inputPath: String): DataFrame = {
      var filterDF: DataFrame = null
      var sourceDF: DataFrame = null
      var inputLocation: String = null

      if (beginStr == null || endStr == null || inputPath == null) {
        return filterDF
      }
      //根据分区加载数据
      if (beginStr.substring(0, 8) == endStr.substring(0, 8)) {
        inputLocation = inputPath + "/d=" + beginStr.substring(2, 8)
      } else {
        val intervalHourNums = (DateUtils.timeInterval(beginStr.substring(0, 8), endStr.substring(0, 8), "yyyyMMdd")) / (3600 * 24)
        for (i <- 0 to intervalHourNums.toInt) {
          val curtime = DateUtils.timeCalcWithFormatConvertSafe(beginStr.substring(0, 8), "yyyyMMdd", i * 60 * 60 * 24, "yyyyMMdd")
          inputLocation = inputPath + "/d=" + curtime.substring(2, 8)
          sourceDF = sqlContext.read.format("orc").load(inputLocation)
          // 分区合并
          if (filterDF == null && sourceDF == null) {
            logInfo("##########--ERROR:filterDF is null and sourceDF is null !!")

          } else if (filterDF == null && sourceDF != null) {
            filterDF = sourceDF
          }
          else if (filterDF != null && sourceDF == null) {
            logInfo("##########--ERROR:filterDF is not null but sourceDF is null !!")
          }
          else if (filterDF != null && sourceDF != null) {
            filterDF = filterDF.unionAll(sourceDF)
          }
        }

      }
      //返回值
      filterDF
    }


  }


      //  //每次执行一个周期，最小周期为小时
      //  if(cyctype == "min"){
      //    intervalbegin = intervals*(-1)
      //    intervalend = intervals*(-1)+60*60
      //    timeformat = "yyyyMMddHHmm"
      //    timeid = begintime.substring(0,10)
      //  }else if(cyctype == "h"){
      //    intervalbegin = intervals*(-1)
      //    intervalend = intervals*(-1)
      //    timeformat = "yyyyMMddHH"
      //    timeid = begintime.substring(0,10)
      //  }else if(cyctype == "d"){
      //    intervalbegin = 0
      //    intervalend = 0
      //    timeformat = "yyyyMMdd"
      //    timeid = begintime.substring(0,8)
      //    datatime = begintime.substring(0,8)
      //  }else if(cyctype == "w"){
      //    intervalbegin = 0
      //    intervalend = 6*24*60*60
      //    timeformat = "yyyyMMdd"
      //    timeid = begintime.substring(0,8)
      //    datatime = begintime.substring(0,8)
      //  }else if(cyctype == "m"){
      //    intervalbegin = 0
      //    val monthid = begintime.substring(4,6)
      //    if(monthid == "02"){
      //      intervalend = 27*24*60*60
      //    }else if(monthid == "01"|| monthid == "03"|| monthid == "05"|| monthid == "07"|| monthid == "08"|| monthid == "10"|| monthid == "12"){
      //      intervalend = 30*24*60*60
      //    }else{
      //      intervalend = 29*24*60*60
      //    }
      //    timeid = begintime.substring(0,6)
      //    timeformat = "yyyyMM"
      //    datatime = begintime.substring(0,6)
      //    dayid = dayid.substring(0,6)
      //  }
      //  beginStr = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,12), "yyyyMMddHHmm", intervalbegin, "yyyyMMddHHmmss")
      //  endStr = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,12), "yyyyMMddHHmm", intervalend, "yyyyMMddHHmmss")
      //
      //  val beginStrH = beginStr
      //  val endStrH = endStr
      //  val beginStrD = beginStr.substring(0,8)
      //  val endStrD = endStr.substring(0,8)
      //
      //  //获取开始时间戳
      //  val fm = new SimpleDateFormat("yyyyMMddHHmmss")
      //  val dt = fm.parse(begintime.substring(0,14))
      //  val begintimestamp = dt.getTime
      //
      //  //获取汇总类型
      //  val ItmeName = sumitemtype.substring(0,sumitemtype.lastIndexOf("_") + 1) //"authlog"
      //  val subItmeName = sumitemtype.substring(sumitemtype.lastIndexOf("_") + 1) //"4gaaa"
      //
      //  logInfo("##########--sumitemtype: " + sumitemtype)
      //  logInfo("##########--begintime: " + begintime)
      //  logInfo("##########--endtimeH: " + endStrH)
      //  logInfo("##########--endtimeD: " + endStrD)
      //  logInfo("##########--begintimestamp: " + begintimestamp)
      //  logInfo("##########--intervals: " + intervals)
      //
      //
      //
      //
      //
      //  //从小时或天表取数据
      //  // 按照粒度汇总数据,一次汇一个周期数据，开始时间作为统计时间，分钟为每小时统计一次,12~13点汇总12点数据
      //  var filterDF:DataFrame =null
      //  var resultDF:DataFrame = null
      //
      //  var outputfsSuffix:String = null
      //  var outputsubLSuffix:String = null
      //  var outputLSuffix:String = null
      //
      //  if(cyctype == "min" || cyctype == "h"){
      //
      //
      //
      //    filterDF = LoadfileByPartitinH(sumitemtype,sqlContext,beginStrH,endStrH,inputPath,intervals)
      //    resultDF = SummarySourceHour(sqlContext,filterDF,sumitemtype,begintimestamp,intervals,userTable,userTablePartitionDayid,timeformat)
      //
      //
      //  }else if(cyctype == "d" || cyctype == "w"|| cyctype == "m"){
      //
      //    filterDF = LoadfileByPartitinD(sqlContext,beginStrD,endStrD,inputPath,intervals)
      //    if(filterDF == null){
      //      logInfo("##########--filterDF: is null")
      //    }
      //    resultDF = SummarySourceDay(sqlContext,filterDF,sumitemtype,datatime,intervals,userTable,userTablePartitionDayid)
      //    if(resultDF == null){
      //      logInfo("##########--resultDF: is null")
      //    }
      //
      //  }
      //
      //
      //
      //  // 文件写入JSON
      //  val coalesceNum = 1
      //  val outputLocatoin = outputPath + "tmp/" +timeid+"/"
      //  //val localpath =  localOutputPath
      //
      //
      //
      //  resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(outputLocatoin)
      //
      //  FileUtils.moveTempFilesToESpath(fileSystem,outputPath,timeid,dayid)
      //  //FileUtils.downFilesToLocal(fileSystem, outputLocatoin, localpath + "/", outputLSuffix, ".json")
      //
      //  sc.stop()
      //
      //}
      //
      //// 按分区加载数据

    //def LoadfileByPartitinD  (sqlContext:SQLContext, begintime:String, endtime:String,inputPath:String,intervals:Int): DataFrame ={
    //  var filterDF:DataFrame = null
    //  if( begintime == "" ||  endtime == "" || inputPath == ""){
    //    logInfo("##########--filterDF is null" )
    //    return filterDF
//
    //  }
    //  val intervalDayNums = (DateUtils.timeInterval(begintime.substring(0,8), endtime.substring(0,8), "yyyyMMdd")) / (3600*24)
    //  var inputLocation = new Array[String](intervalDayNums.toInt+1)
    //  if (begintime.substring(0,8) == endtime.substring(0,8)) {
    //    inputLocation(0) = inputPath + "/d=" + begintime.substring(2,8)
    //    logInfo("##########--inputpath:" +  inputLocation(0))
    //    filterDF = sqlContext.read.format("orc").load(inputLocation(0))
    //    if(filterDF == null){
    //      logInfo("##########--filterDF: is null")
    //    }
    //  }else {
//
    //    for (i <- 0 to intervalDayNums.toInt) {
    //      val curD = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,8), "yyyyMMdd", i * 60 * 60*24, "yyMMdd")
    //      inputLocation(i) = inputPath + "/d=" + curD.substring(2)
    //      logInfo("##########--inputpath:" + i +"--" + inputLocation(i))
    //      val sourceDF = sqlContext.read.format("orc").load(inputLocation(i))
//
    //      if (filterDF==null && sourceDF==null)
    //      {
    //        logInfo("##########--ERROR:filterDF is null and sourceDF is null !!")
//
    //      }else if(filterDF==null && sourceDF != null){
    //        filterDF=sourceDF
    //      }
    //      else if(filterDF!=null && sourceDF == null){
    //        logInfo("##########--ERROR:filterDF is not null but sourceDF is null !!")
    //      }
    //      else if(filterDF !=null && sourceDF != null){
    //        filterDF = filterDF.unionAll(sourceDF)
    //      }
    //    }
//
    //  }
    //  filterDF
//
    //}
    //def SummarySourceHour (sqlContext:SQLContext,filterDF:DataFrame,ItmeName:String,btimestamp:Long,intervals:Int,userTable:String,userTablePartitionDayid:String,timeformat:String):DataFrame={
//
    //  var resultDF:DataFrame = null
    //  var companyDF:DataFrame = null
    //  if(ItmeName == "authlog_3gaaa"){
    //    resultDF = filterDF.groupBy(
    //      when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
    //      when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
    //      from_unixtime(ceil((unix_timestamp(filterDF.col("authtime"),"yyyyMMddHHmmss") - btimestamp)/intervals)*intervals+btimestamp,s"$timeformat").as("datetime"),
    //      filterDF.col("result"),
    //      filterDF.col("auth_result").as("errorcode")).
    //      agg(  count(lit(1)).alias("requirecnt"),
    //        sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
    //        countDistinct((when(filterDF.col("result")==="failed",filterDF.col("imsicdma")))).as("errmdncnt"),
    //        countDistinct(filterDF.col("imsicdma")).as("mdncnt")
    //      )
    //  }else if(ItmeName == "authlog_4gaaa" || ItmeName== "authlog_vpdn"){
    //    if(filterDF == null){
    //      logInfo(s"##########--${ItmeName}: resultDF is null")
    //    }
    //    resultDF = filterDF.groupBy(
    //      when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
    //      when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
    //      from_unixtime(ceil((unix_timestamp(filterDF.col("authtime"),"yyyyMMddHHmmss") - btimestamp)/intervals)*intervals+btimestamp,s"$timeformat").as("datetime"),
    //      filterDF.col("result"),
    //      filterDF.col("auth_result").as("errorcode")).
    //      agg(  count(lit(1)).alias("requirecnt"),
    //        sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
    //        countDistinct((when(filterDF.col("result")==="failed",filterDF.col("mdn")))).as("errmdncnt"),
    //        countDistinct(filterDF.col("mdn")).as("mdncnt")
    //      )
    //  }else if(ItmeName == "mme"){
//
    //    resultDF = filterDF.groupBy(
    //      when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
    //      when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
    //      filterDF.col("province"),
    //      when(filterDF.col("devicetype").isNull,"").otherwise(filterDF.col("devicetype")).alias("devicetype"),
    //      when(filterDF.col("modelname").isNull,"").otherwise(filterDF.col("modelname")).alias("modelname"),
    //      from_unixtime(ceil((unix_timestamp(filterDF.col("starttime"),"yyyy-MM-dd HH:mm:ss.SSS") - btimestamp)/intervals)*intervals+btimestamp,s"$timeformat").as("datetime"),
    //      filterDF.col("result"),
    //      filterDF.col("pcause").as("errorcode")).
    //      agg(  count(lit(1)).alias("requirecnt"),
    //        sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
    //        countDistinct((when(filterDF.col("result")==="failed",filterDF.col("mdn")))).as("errmdncnt"),
    //        countDistinct(filterDF.col("mdn")).as("mdncnt")
    //      )
    //  }else if(ItmeName == "flow"){
//
//
    //  }
//
//
//
//
    //  resultDF
    //}
//
//
    //def SummarySourceDay (sqlContext:SQLContext,filterDF:DataFrame,ItmeName:String,dayid:String,intervals:Int,userTable:String,userTablePartitionDayid:String):DataFrame={
//
    //  var resultDF:DataFrame = null
//
    //  if(ItmeName == "authlog_3gaaa"){
    //    resultDF = filterDF.groupBy(
    //      when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
    //      when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
    //      filterDF.col("result"),
    //      filterDF.col("auth_result").as("errorcode")).
    //      agg(  count(lit(1)).alias("requirecnt"),
    //        sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
    //        countDistinct((when(filterDF.col("result")==="failed",filterDF.col("imsicdma")))).as("errmdncnt"),
    //        countDistinct(filterDF.col("imsicdma")).as("mdncnt")
    //      ).withColumn("datetime",lit(dayid))
    //  }else if(ItmeName == "authlog_4gaaa" || ItmeName== "authlog_vpdn"){
    //    resultDF = filterDF.groupBy(
    //      when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
    //      when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
    //      filterDF.col("result"),
    //      filterDF.col("auth_result").as("errorcode")).
    //      agg(  count(lit(1)).alias("requirecnt"),
    //        sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
    //        countDistinct((when(filterDF.col("result")==="failed",filterDF.col("mdn")))).as("errmdncnt"),
    //        countDistinct(filterDF.col("mdn")).as("mdncnt")
    //      ).withColumn("datetime",lit(dayid))
    //  }else if(ItmeName == "mme"){
    //    resultDF = filterDF.groupBy(
    //      when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
    //      when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
    //      filterDF.col("province"),
    //      when(filterDF.col("devicetype").isNull,"").otherwise(filterDF.col("devicetype")).alias("devicetype"),
    //      when(filterDF.col("modelname").isNull,"").otherwise(filterDF.col("modelname")).alias("modelname"),
    //      filterDF.col("result"),
    //      filterDF.col("pcause").as("errorcode")).
    //      agg(  count(lit(1)).alias("requirecnt"),
    //        sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
    //        countDistinct((when(filterDF.col("result")==="failed",filterDF.col("mdn")))).as("errmdncnt"),
    //        countDistinct(filterDF.col("mdn")).as("mdncnt")
    //      ).withColumn("datetime",lit(dayid))
    //  }else if(ItmeName == "flow"){
    //    resultDF
//
    //  }
//
//
//
    //  resultDF
    //}
//
  //}
//
//}
