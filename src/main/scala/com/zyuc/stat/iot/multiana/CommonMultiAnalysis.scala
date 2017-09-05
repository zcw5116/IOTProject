package com.zyuc.stat.iot.multiana

import java.text.SimpleDateFormat

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, FileUtils}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by dell on 2017/8/28.
  */
object CommonMultiAnalysis extends Logging {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val sumitemtype = sc.getConf.get("spark.app.item.type") //"authlog_3gaaa;authlog_4gaaa,authlog_pdsn"
    val begintime = sc.getConf.get("spark.app.begintime") //20170826010000 yyyymmddhhmiss
    val endtime = sc.getConf.get("spark.app.endtime")//20170826020000 yyyymmddhhmiss
    val interval = sc.getConf.get("spark.app.interval")//15
    val cyctype = sc.getConf.get("spark.app.cyctype")// min/h/d/w/m
    val userTable = sc.getConf.get("spark.app.user.table")     // "iot_customer_userinfo"
    val userTablePartitionDayid = sc.getConf.get("spark.app.user.userTablePatitionDayid")  //  "20170801"
    val inputPath = sc.getConf.get("spark.app.inputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/authlog/15min/;
    val outputPath = sc.getConf.get("spark.app.outputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/authlog/15min/;
    val localOutputPath =  sc.getConf.get("spark.app.jsonpath") // /slview/test/limm/multiAna/authlog/15min/json/;


    //判断统计起始和截止时间
    val intervals = interval.toInt * 60
    var beginStrH:String = null
    var endStrH:String = null
    var intervalbegin:Int = 0
    var intervalend:Int = 0
    //每次执行一个小时
    if(cyctype == "min"){
      intervalbegin = intervals*(-1)
      intervalend = intervals*(-1) + 60*60
    }else if(cyctype == "h"){
      intervalbegin = intervals*(-1)
      intervalend = intervals*(-1)
    }
    beginStrH = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,12), "yyyyMMddHHmm", intervalbegin, "yyyyMMddHHmmss")
    endStrH = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,12), "yyyyMMddHHmm", intervalend, "yyyyMMddHHmmss")

    //val beginStrD = DateUtils.timeCalcWithFormatConvertSafe(begintime, "yyyyMMdd", 0, "yyyyMMddHHmmss")
    //val endStrD = DateUtils.timeCalcWithFormatConvertSafe(endtime, "yyyyMMdd", 0, "yyyyMMddHHmmss")

    val beginStrD = begintime.substring(0,8)
    val endStrD = endtime.substring(0,8)

    //获取开始时间戳
    val fm = new SimpleDateFormat("yyyyMMddHHmmss")
    val dt = fm.parse(beginStrH)
    val begintimestamp = dt.getTime


    //获取汇总类型
    val ItmeName = sumitemtype.substring(0,sumitemtype.lastIndexOf("_") + 1) //"authlog"
    val subItmeName = sumitemtype.substring(sumitemtype.lastIndexOf("_") + 1) //"4gaaa"




    //从小时或天表取数据
    // 按照粒度汇总数据,一次汇一个周期数据，开始时间作为统计时间，分钟为每小时统计一次,12~13点汇总12点数据
    var filterDF:DataFrame =null
    var resultDF:DataFrame = null
    var timeid:String = null
    var outputfsSuffix:String = null
    var outputsubLSuffix:String = null
    var outputLSuffix:String = null
    var timeformat:String = null
    if(cyctype == "min" || cyctype == "h"){
      if(cyctype=="min" ){
        timeid = begintime.substring(0,12)
        timeformat = "yyyyMMddHHmm"
      }else if(cyctype == "h"){
        timeid = begintime.substring(0,10)
        timeformat = "yyyyMMddHH"
      }

      logInfo("##########--begintime: " + beginStrH)
      logInfo("##########--endtime: " + endStrH)
      logInfo("##########--begintimestamp: " + begintimestamp)
      logInfo("##########--intervals: " + intervals)
      filterDF = LoadfileByPartitinH(sumitemtype,sqlContext,beginStrH,endStrH,inputPath,intervals)
      resultDF = SummarySourceHour(sqlContext,filterDF,sumitemtype,begintimestamp,intervals,userTable,userTablePartitionDayid,timeformat)
      outputfsSuffix = begintime.substring(0,8) + "/" + begintime.substring(8,10)
      outputsubLSuffix =  "/"+ begintime.substring(0,8)
      outputLSuffix = begintime.substring(8,10)
    }else if(cyctype == "d" || cyctype == "w"|| cyctype == "m"){
      if(cyctype == "d"){
        timeid = begintime.substring(0,8)
      }else if(cyctype == "w"){
        timeid = begintime.substring(0,8)
      }else if(cyctype == "w"){
        timeid = begintime.substring(0,6)
      }
      outputfsSuffix = timeid
      outputLSuffix = timeid
      logInfo("##########--begintime: " + beginStrD)
      logInfo("##########--endtime: " + endStrD)
      logInfo("##########--sumitemtype: " + sumitemtype)
      logInfo("##########--timeid: " + timeid)
      filterDF = LoadfileByPartitinD(sqlContext,beginStrD,endStrD,inputPath,intervals)
      if(filterDF == null){
        logInfo("##########--filterDF: is null")
      }
      resultDF = SummarySourceDay(sqlContext,filterDF,sumitemtype,timeid,intervals,userTable,userTablePartitionDayid)
      if(resultDF == null){
        logInfo("##########--resultDF: is null")
      }
      outputsubLSuffix ="/"
    }


    // 文件写入JSON
    val coalesceNum = 1
    val outputLocatoin = outputPath + "json/data/" +outputfsSuffix
    val localpath =  localOutputPath + outputsubLSuffix

    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(outputLocatoin)

    FileUtils.downFilesToLocal(fileSystem, outputLocatoin, localpath + "/", outputLSuffix, ".json")

    sc.stop()

  }
  // 按分区加载数据
  def LoadfileByPartitinH  (partitioncolname:String,sqlContext:SQLContext, begintime:String, endtime:String,inputPath:String,intervals:Int): DataFrame ={
    var filterDF:DataFrame = null
    var sourceDF:DataFrame = null
    //获取汇总在分区的时间字段

    if( begintime == "" ||  endtime == "" || inputPath == ""){
      return filterDF
    }
      val intervalHourNums = (DateUtils.timeInterval(begintime.substring(0,10), endtime.substring(0,10), "yyyyMMddHH")) / 3600
      var inputLocation = new Array[String](intervalHourNums.toInt+1)
      logInfo("##########--intervalHourNum: " + intervalHourNums)
      if (begintime.substring(2,10) == endtime.substring(2,10)) {
        inputLocation(0) = inputPath + "/d=" + begintime.substring(2,8) + "/h=" + begintime.substring(8,10)
        //inputLocation(0) = inputPath + "/d=" + begintime.substring(2,8) + "/h=" + begintime.substring(9,10)
        logInfo("##########--inputpath: " + inputLocation(0))
      filterDF = sqlContext.read.format("orc").load(inputLocation(0))
     }else {

       for (i <- 0 to intervalHourNums.toInt) {
         val curtime = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,10), "yyyyMMddHH", i * 60 * 60, "yyyyMMddHH")
         val curH = curtime.substring(8)
         val curD = curtime.substring(0,8)
         inputLocation(i) = inputPath + "/d=" + curD.substring(2) + "/h=" + curH
         //inputLocation(i) = inputPath + "/d=" + curD.substring(2) + "/h=" + curH.substring(1)
         logInfo("##########--inputpath:" + i +"--" + inputLocation(i))
         //logInfo("##########--begintime:" + i +"--" + begintime
         //if(i==0){
         //   sourceDF = sqlContext.read.format("orc").load(inputLocation(i))
         //}else if(i == intervalHourNums){
         //   sourceDF = sqlContext.read.format("orc").load(inputLocation(i))
         //}else{
         //   sourceDF = sqlContext.read.format("orc").load(inputLocation(i))
         //}
         sourceDF = sqlContext.read.format("orc").load(inputLocation(i))
         if (filterDF==null && sourceDF==null)
         {
           logInfo("##########--ERROR:filterDF is null and sourceDF is null !!")

         }else if(filterDF==null && sourceDF != null){
           filterDF=sourceDF
         }
         else if(filterDF!=null && sourceDF == null){
           logInfo("##########--ERROR:filterDF is not null but sourceDF is null !!")
         }
         else if(filterDF!=null && sourceDF != null){
           filterDF = filterDF.unionAll(sourceDF)
         }

       }
        if(partitioncolname == "authlog_3gaaa" || partitioncolname == "authlog_4gaaa"  || partitioncolname == "authlog_vpdn"  ){

          filterDF = filterDF.filter("authtime>=" + begintime).
            filter( "authtime<" + endtime)

        }else if(partitioncolname == "mme"){
          filterDF = filterDF.filter(s"from_unixtime(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss.SSS'),'yyyyMMddHHmmss')>= $begintime").
            filter( s"from_unixtime(unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss.SSS'),'yyyyMMddHHmmss')< $endtime")

        }

      }
      filterDF

  }

  def LoadfileByPartitinD  (sqlContext:SQLContext, begintime:String, endtime:String,inputPath:String,intervals:Int): DataFrame ={
    var filterDF:DataFrame = null
    if( begintime == "" ||  endtime == "" || inputPath == ""){
      logInfo("##########--filterDF is null" )
      return filterDF

    }
    val intervalDayNums = (DateUtils.timeInterval(begintime.substring(0,8), endtime.substring(0,8), "yyyyMMdd")) / (3600*24)
    var inputLocation = new Array[String](intervalDayNums.toInt+1)
    if (begintime.substring(0,8) == endtime.substring(0,8)) {
      inputLocation(0) = inputPath + "/d=" + begintime.substring(2,8)
      logInfo("##########--inputpath:" +  inputLocation(0))
      filterDF = sqlContext.read.format("orc").load(inputLocation(0))
      if(filterDF == null){
        logInfo("##########--filterDF: is null")
      }
    }else {

      for (i <- 0 to intervalDayNums.toInt) {
        val curD = DateUtils.timeCalcWithFormatConvertSafe(begintime.substring(0,8), "yyyyMMdd", i * 60 * 60*24, "yyMMdd")
         inputLocation(i) = inputPath + "/d=" + curD.substring(2)
        logInfo("##########--inputpath:" + i +"--" + inputLocation(i))
        val sourceDF = sqlContext.read.format("orc").load(inputLocation(i))

        if (filterDF==null && sourceDF==null)
        {
          logInfo("##########--ERROR:filterDF is null and sourceDF is null !!")

        }else if(filterDF==null && sourceDF != null){
          filterDF=sourceDF
        }
        else if(filterDF!=null && sourceDF == null){
          logInfo("##########--ERROR:filterDF is not null but sourceDF is null !!")
        }
        else if(filterDF !=null && sourceDF != null){
          filterDF = filterDF.unionAll(sourceDF)
        }
      }

    }
    filterDF

  }
  def SummarySourceHour (sqlContext:SQLContext,filterDF:DataFrame,ItmeName:String,btimestamp:Long,intervals:Int,userTable:String,userTablePartitionDayid:String,timeformat:String):DataFrame={

    var resultDF:DataFrame = null
    var companyDF:DataFrame = null
    if(ItmeName == "authlog_3gaaa"){
      resultDF = filterDF.groupBy(
        when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
        when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
        from_unixtime(ceil((unix_timestamp(filterDF.col("authtime"),"yyyyMMddHHmmss") - btimestamp)/intervals)*intervals+btimestamp,s"$timeformat").as("datetime"),
        filterDF.col("result"),
        filterDF.col("auth_result").as("errorcode")).
        agg(  count(lit(1)).alias("requirecnt"),
          sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
          countDistinct((when(filterDF.col("result")==="failed",filterDF.col("imsicdma")))).as("errmdncnt"),
          countDistinct(filterDF.col("imsicdma")).as("mdncnt")
        )
    }else if(ItmeName == "authlog_4gaaa" || ItmeName== "authlog_vpdn"){
      if(filterDF == null){
        logInfo(s"##########--${ItmeName}: resultDF is null")
      }
      resultDF = filterDF.groupBy(
        when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
        when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
        from_unixtime(ceil((unix_timestamp(filterDF.col("authtime"),"yyyyMMddHHmmss") - btimestamp)/intervals)*intervals+btimestamp,s"$timeformat").as("datetime"),
        filterDF.col("result"),
        filterDF.col("auth_result").as("errorcode")).
        agg(  count(lit(1)).alias("requirecnt"),
          sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
          countDistinct((when(filterDF.col("result")==="failed",filterDF.col("mdn")))).as("errmdncnt"),
          countDistinct(filterDF.col("mdn")).as("mdncnt")
        )
    }else if(ItmeName == "mme"){

      resultDF = filterDF.groupBy(
        when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
        when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
        filterDF.col("province"),
        when(filterDF.col("devicetype").isNull,"").otherwise(filterDF.col("devicetype")).alias("devicetype"),
        when(filterDF.col("modelname").isNull,"").otherwise(filterDF.col("modelname")).alias("modelname"),
        from_unixtime(ceil((unix_timestamp(filterDF.col("starttime"),"yyyy-MM-dd HH:mm:ss.SSS") - btimestamp)/intervals)*intervals+btimestamp,s"$timeformat").as("datetime"),
        filterDF.col("result"),
        filterDF.col("pcause").as("errorcode")).
        agg(  count(lit(1)).alias("requirecnt"),
          sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
          countDistinct((when(filterDF.col("result")==="failed",filterDF.col("mdn")))).as("errmdncnt"),
          countDistinct(filterDF.col("mdn")).as("mdncnt")
        )
    }else if(ItmeName == "flow"){



    }




    resultDF
  }


  def SummarySourceDay (sqlContext:SQLContext,filterDF:DataFrame,ItmeName:String,dayid:String,intervals:Int,userTable:String,userTablePartitionDayid:String):DataFrame={

    var resultDF:DataFrame = null

    if(ItmeName == "authlog_3gaaa"){
      resultDF = filterDF.groupBy(
        when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
        when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
        filterDF.col("result"),
        filterDF.col("auth_result").as("errorcode")).
        agg(  count(lit(1)).alias("requirecnt"),
          sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
          countDistinct((when(filterDF.col("result")==="failed",filterDF.col("imsicdma")))).as("errmdncnt"),
          countDistinct(filterDF.col("imsicdma")).as("mdncnt")
        ).withColumn("datetime",lit(dayid))
    }else if(ItmeName == "authlog_4gaaa" || ItmeName== "authlog_vpdn"){
      resultDF = filterDF.groupBy(
        when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
        when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
        filterDF.col("result"),
        filterDF.col("auth_result").as("errorcode")).
        agg(  count(lit(1)).alias("requirecnt"),
          sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
          countDistinct((when(filterDF.col("result")==="failed",filterDF.col("mdn")))).as("errmdncnt"),
          countDistinct(filterDF.col("mdn")).as("mdncnt")
        ).withColumn("datetime",lit(dayid))
    }else if(ItmeName == "mme"){
      resultDF = filterDF.groupBy(
        when (length(filterDF.col("custprovince"))===0,"其他").when(filterDF.col("custprovince").isNull,"其他").otherwise(filterDF.col("custprovince")).alias("custprovince"),
        when (length(filterDF.col("vpdncompanycode"))===0,"N999999999").when(filterDF.col("vpdncompanycode").isNull,"N999999999").otherwise(filterDF.col("vpdncompanycode")).alias("companycode"),
        filterDF.col("province"),
        when(filterDF.col("devicetype").isNull,"").otherwise(filterDF.col("devicetype")).alias("devicetype"),
        when(filterDF.col("modelname").isNull,"").otherwise(filterDF.col("modelname")).alias("modelname"),
        filterDF.col("result"),
        filterDF.col("pcause").as("errorcode")).
        agg(  count(lit(1)).alias("requirecnt"),
          sum(when (filterDF.col("result")==="failed",1).otherwise(0)).alias("errcnt"),
          countDistinct((when(filterDF.col("result")==="failed",filterDF.col("mdn")))).as("errmdncnt"),
          countDistinct(filterDF.col("mdn")).as("mdncnt")
        ).withColumn("datetime",lit(dayid))
    }else if(ItmeName == "flow"){
      resultDF

    }



    resultDF
  }




}
