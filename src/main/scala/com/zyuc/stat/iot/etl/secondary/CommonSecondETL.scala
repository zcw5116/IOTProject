package com.zyuc.stat.iot.etl.secondary

import com.zyuc.stat.iot.etl.util.CommonETLUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.makeCoalesce
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.{col, lit, regexp_replace, when}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by limm on 2017/9/14.
  */
object CommonSecondETL extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use"+ ConfigProperties.IOT_HIVE_DATABASE)
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    sqlContext.setConf("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    // 入参处理
    val appName = sc.getConf.get("spark.app.name") // name_{}_2017073111
    val inputPath = sc.getConf.get("spark.app.inputPath") // "hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/data/cdr/output/pdsn/data/"
    val outputPath = sc.getConf.get("spark.app.outputPath") //"hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/data/cdr/secondaryoutput/pdsn/"
    val itemType = sc.getConf.get("spark.app.item.type") // pdsn,pgw,haccg
    val FirstETLTable = sc.getConf.get("spark.app.table.source") // "iot_cdr_data_pdsn"
    val SecondaryETLTable = sc.getConf.get("spark.app.table.stored") // "iot_cdr_data_pdsn_h"
    val timeid = sc.getConf.get("spark.app.timeid")//yyyymmddhhmiss
    val cyctype = sc.getConf.get("spark.app.interval")
    val terminaltable = sc.getConf.get("spark.app.table.terminal")
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt //128

    //入参打印
    logInfo("##########--inputPath:   " + inputPath)
    logInfo("##########--outputPath:  " + outputPath)
    logInfo("##########--storedtable: " + SecondaryETLTable)
    logInfo("##########--itemType:    " + itemType)
    logInfo("##########--timeid:      "  + timeid)
    logInfo("##########--cyctype:     " + cyctype)


    // 清洗粒度 d,h
    val hourid = timeid.substring(0,10)
    val dayid  = timeid.substring(0,8)
    val partitionD = timeid.substring(2, 8)
    val partitionH = timeid.substring(8,10)
    //itemType

    val ItmeName = itemType.substring(0,itemType.lastIndexOf("_") + 1) //"authlog"
    val subItmeName = itemType.substring(itemType.lastIndexOf("_") + 1) //"4g"


    // 待清洗文件路径
    var partitions:String = null
    var sourceinputPath:String = inputPath
    var subPath:String = null
    if(cyctype == 'd'){
      partitions = "d"
      subPath = dayid
      sourceinputPath =  inputPath + "/d=" + partitionD
    }else if (cyctype == 'h'){
      partitions = "d,h"
      subPath = hourid
      if(ItmeName=="authlog"){
        sourceinputPath = inputPath + "/dayid=" + dayid + "/hourid=" + partitionH
      }else{
        sourceinputPath = inputPath + "/d=" + partitionD + "/h=" + partitionH
      }
    }

    logInfo("##########--sourceinputPath:   " + sourceinputPath)

    // 字段处理
    val sourceDF = sqlContext.read.format("orc").load(sourceinputPath)
    var resultDF:DataFrame = null
    resultDF = DealSourceDate(sqlContext,sourceDF,itemType,cyctype,partitionD,partitionH,terminaltable)

    if(resultDF == null){
      logInfo("##########--ERROR:resultDF is null,please check it...  " )
      return
    }

    // 文件切片
    val coalesceNum = makeCoalesce(fileSystem, sourceinputPath, coalesceSize)

    // 将数据存入到HDFS， 并刷新分区表
    CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, resultDF, coalesceNum, partitions,subPath, outputPath, SecondaryETLTable, appName)


  }



  def DealSourceDate(sqlContext:SQLContext, sourceDF:DataFrame, itemType:String, cyctype:String, partitionD:String, partitionH:String, terminaltable:String): DataFrame ={
    var resultDF:DataFrame = null
    var userDF:DataFrame = null
    // authlog_3g
    if(itemType=="authlog_3g"){
      if(cyctype =="h") {
        resultDF = sourceDF.select(sourceDF.col("auth_result"), sourceDF.col("device"), sourceDF.col("imsicdma"),
          sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"), sourceDF.col("nasport"),
          sourceDF.col("nasportid"), sourceDF.col("nasporttype"), sourceDF.col("pcfip"), sourceDF.col("srcip")).
          withColumn("authtime", regexp_replace(col("auth_time"), "[: -]", "")).
          withColumn("result", when(col("auth_result") === 0, "success").otherwise("failed")).
          withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))
      }

      if(cyctype =="d"){
        resultDF = sourceDF.select(sourceDF.col("auth_result"), sourceDF.col("device"), sourceDF.col("imsicdma"),sourceDF.col("authtime"),
          sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"), sourceDF.col("nasport"),
          sourceDF.col("nasportid"), sourceDF.col("nasporttype"), sourceDF.col("pcfip"), sourceDF.col("srcip"),
          sourceDF.col("result")).withColumn("d", lit(partitionD))

      }
    }
    // authlog_4g

    if(itemType=="authlog_4g"){
      if(cyctype =="h") {
        resultDF = sourceDF.select(sourceDF.col("auth_result"), sourceDF.col("device"), sourceDF.col("imsicdma"),
          sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"), sourceDF.col("nasportid"),
          sourceDF.col("nasporttype"), sourceDF.col("pcfip")).
          withColumn("authtime", regexp_replace(col("auth_time"), "[: -]", "")).
          withColumn("result", when(col("auth_result") === 0, "success").otherwise("failed")).
          withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))
      }

      if(cyctype =="d"){
        resultDF = sourceDF.
          select(sourceDF.col("auth_result"), sourceDF.col("authtime"), sourceDF.col("device"), sourceDF.col("imsicdma"),
            sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"),
            sourceDF.col("nasportid"), sourceDF.col("nasporttype"), sourceDF.col("pcfip"),
            sourceDF.col("result")).withColumn("d", lit(partitionD))

      }


    }

    // authlog_vpdn
    if(itemType=="authlog_vpdn"){

      if(cyctype =="h") {
        resultDF = sourceDF.select(sourceDF.col("auth_result"), sourceDF.col("device"), sourceDF.col("imsicdma"),
          sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"), sourceDF.col("entname"),
          sourceDF.col("lnsip"), sourceDF.col("pdsnip")).
          withColumn("authtime", regexp_replace(col("auth_time"), "[: -]", "")).
          withColumn("result", when(col("auth_result") === 0, "success").otherwise("failed")).
          withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))
      }

      if(cyctype =="d"){
        resultDF = sourceDF.
          select(sourceDF.col("auth_result"), sourceDF.col("authtime"), sourceDF.col("device"), sourceDF.col("imsicdma"),
            sourceDF.col("imsilte"), sourceDF.col("mdn"), sourceDF.col("nai_sercode"),
            sourceDF.col("entname"), sourceDF.col("lnsip"), sourceDF.col("pdsnip"),
            sourceDF.col("result")).withColumn("d", lit(partitionD))

      }

    }
    // mme
    if(itemType=="mme"){
      //terminaltable = "iot_dim_terminal"



      if(cyctype =="h") {
        val terminalDF = sqlContext.table(terminaltable).select("tac", "modelname", "devicetype").cache()
        resultDF = sourceDF.join(terminalDF, sourceDF.col("imei").substr(0, 8) === terminalDF.col("tac"), "left").
          select(sourceDF.col("msisdn").as("mdn"), sourceDF.col("province"), sourceDF.col("imei"), sourceDF.col("procedureid"),
            sourceDF.col("acctype"), sourceDF.col("imsi"), sourceDF.col("uemme"),
            sourceDF.col("pcause"), sourceDF.col("ci"), sourceDF.col("enbid"), sourceDF.col("uemme"),
            sourceDF.col("newgrpid"), sourceDF.col("newmmecode"), sourceDF.col("newmtmsi"), sourceDF.col("mmetype"),
            sourceDF.col("result"),
            terminalDF.col("tac"), terminalDF.col("modelname"), terminalDF.col("devicetype")
          ).withColumn("starttime", regexp_replace(col("starttime"), "[: -]", "").substr(0,14)).
          withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))
      }
      if(cyctype =="d"){
        resultDF = sourceDF.
          select(sourceDF.col("mdn"), sourceDF.col("province"), sourceDF.col("imei"), sourceDF.col("procedureid"),
            sourceDF.col("starttime"), sourceDF.col("acctype"), sourceDF.col("imsi"), sourceDF.col("uemme"),
            sourceDF.col("pcause"), sourceDF.col("ci"), sourceDF.col("enbid"), sourceDF.col("uemme"),
            sourceDF.col("newgrpid"), sourceDF.col("newmmecode"), sourceDF.col("newmtmsi"), sourceDF.col("mmetype"),
            sourceDF.col("result"), sourceDF.col("tac"), sourceDF.col("modelname"), sourceDF.col("devicetype")
          ).withColumn("d", lit(partitionD))

      }

    }

    // cdr_pgw
    if(itemType=="cdr_pgw"){


      if(cyctype =="h"){
        resultDF =  sourceDF.select(sourceDF.col("mdn"),sourceDF.col("recordtype"),sourceDF.col("starttime"),sourceDF.col("stoptime"),
          sourceDF.col("l_timeoffirstusage"),sourceDF.col("l_timeoflastusage"),sourceDF.col("l_datavolumefbcuplink").alias("upflow"),
          sourceDF.col("l_datavolumefbcdownlink").alias("downflow")
        ).withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))

      }
      if(cyctype =="d"){
        resultDF =sourceDF.select(sourceDF.col("mdn"),sourceDF.col("recordtype"),sourceDF.col("starttime"),sourceDF.col("stoptime"),
          sourceDF.col("l_timeoffirstusage"),sourceDF.col("l_timeoflastusage"),sourceDF.col("upflow"),
          sourceDF.col("downflow")
        ).withColumn("d", lit(partitionD))

      }

    }

    // cdr_pdsn
    if(itemType=="cdr_pdsn"){

      if(cyctype =="h"){
        resultDF = sourceDF.select(sourceDF.col("mdn"),sourceDF.col("account_session_id"),sourceDF.col("acct_status_type"),
          sourceDF.col("originating").alias("upflow"),sourceDF.col("termination").alias("downflow"),sourceDF.col("event_time"),sourceDF.col("active_time"),
          sourceDF.col("acct_input_packets"),sourceDF.col("acct_output_packets"),sourceDF.col("acct_session_time"),
          sourceDF.col("vpdncompanycode"),sourceDF.col("custprovince"),sourceDF.col("cellid"),sourceDF.col("bsid")).
          withColumn("d", lit(partitionD)).withColumn("h", lit(partitionH))
      }

      if(cyctype =="d"){
        resultDF = sourceDF.select(sourceDF.col("mdn"),sourceDF.col("account_session_id"),sourceDF.col("acct_status_type"),
          sourceDF.col("upflow"),sourceDF.col("downflow"),sourceDF.col("event_time"),sourceDF.col("active_time"),
          sourceDF.col("acct_input_packets"),sourceDF.col("acct_output_packets"),sourceDF.col("acct_session_time"),
          sourceDF.col("vpdncompanycode"),sourceDF.col("custprovince"),sourceDF.col("cellid"),sourceDF.col("bsid")).
          withColumn("d", lit(partitionD))

      }


    }



    resultDF


  }


}
