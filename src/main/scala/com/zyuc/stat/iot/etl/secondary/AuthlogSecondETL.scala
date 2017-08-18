package com.zyuc.stat.iot.etl.secondary

import java.util.Date

import com.zyuc.stat.iot.etl.util.CommonETLUtils
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils.makeCoalesce
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by zhoucw on 17-8-1.
  */
object AuthlogSecondETL extends Logging {

  val AUTH_LOGTYPE_3G = "3g"
  val AUTH_LOGTYPE_4G = "4g"
  val AUTH_LOGTYPE_VPDN = "vpdn"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName = sc.getConf.get("spark.app.name") // name_2017073111
    val inputPath = sc.getConf.get("spark.app.auth.inputPath") // "hdfs://EPC-LOG-NM-15:8020/user/hive/warehouse/iot.db/iot_userauth_3gaaa/"
    val outputPath = sc.getConf.get("spark.app.outputPath") //"hdfs://EPC-LOG-NM-15:8020/hadoop/IOT/ANALY_PLATFORM/AuthLog/secondETLData/"
    val authlogType = sc.getConf.get("spark.app.auth.logtype") //"3g"
    val userTable = sc.getConf.get("spark.app.user.table") //"iot_customer_userinfo"
    val authLogTable = sc.getConf.get("spark.app.firstETL.auth.table") // "iot_userauth_3gaaa"
    val authLogDayHourTable = sc.getConf.get("spark.app.secondETL.auth.table") // "iot_userauth_3gaaa_day_hour"
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt //128

    if (authlogType != AUTH_LOGTYPE_3G && authlogType != AUTH_LOGTYPE_4G && authlogType != AUTH_LOGTYPE_VPDN) {
      logError("[" + appName + "] 日志类型authlogType错误, 期望值： " + AUTH_LOGTYPE_3G + "," + AUTH_LOGTYPE_4G + "," + AUTH_LOGTYPE_VPDN)
      return
    }

    val hourid = appName.substring(appName.lastIndexOf("_") + 1) //"2017073111"

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    // sqlContext.sql("set spark.sql.shuffle.partitions=500")

    val partitionD = hourid.substring(0, 8)
    val partitionH = hourid.substring(8, 10)
    val preDayid = hourid.substring(0, 8)


    // mme第一次清洗保存到位置
    val inputLocation = inputPath + "/dayid=" + partitionD + "/hourid=" + partitionH
    try {


      var begin = new Date().getTime

      import org.apache.spark.sql.functions._
      // authlog第一次清洗保存到位置
      var authDF = sqlContext.table(authLogTable).filter("dayid=" + partitionD).filter("hourid=" + partitionH).withColumn("authtime", regexp_replace(col("auth_time"), "[: -]", "")).
        withColumnRenamed("hourid", "h").withColumn("d", col("dayid").substr(3, 8))


      val userDF = sqlContext.table(userTable).filter("d=" + preDayid).
        selectExpr("mdn", "imsicdma", "custprovince", "case when length(vpdncompanycode)=0 then 'N999999999' else vpdncompanycode end  as vpdncompanycode").
        cache()


      def getAuthLogDF(authlogType: String): DataFrame = {
        // 关联出字段, userDF: vpdncompanycode, custprovince
        var resultDF: DataFrame = null
        if (authlogType == AUTH_LOGTYPE_3G) {

          authDF = authDF.withColumn("result", when(col("auth_result") === 0, "success").otherwise("failed"))

          resultDF = authDF.join(userDF, userDF.col("imsicdma") === authDF.col("imsicdma"), "left").
            select(authDF.col("auth_result"), authDF.col("authtime"), authDF.col("device"), authDF.col("imsicdma"),
              authDF.col("imsilte"), authDF.col("mdn"), authDF.col("nai_sercode"), authDF.col("nasport"),
              authDF.col("nasportid"), authDF.col("nasporttype"), authDF.col("pcfip"), authDF.col("srcip"),
              authDF.col("result"), userDF.col("vpdncompanycode"), userDF.col("custprovince"),
              authDF.col("d"), authDF.col("h"))

        } else if (authlogType == AUTH_LOGTYPE_4G) {

          authDF = authDF.withColumn("result", when(col("auth_result") === 0, "success").otherwise("failed"))

          resultDF = authDF.join(userDF, userDF.col("mdn") === authDF.col("mdn"), "left").
            select(authDF.col("auth_result"), authDF.col("authtime"), authDF.col("device"), authDF.col("imsicdma"),
              authDF.col("imsilte"), authDF.col("mdn"), authDF.col("nai_sercode"),
              authDF.col("nasportid"), authDF.col("nasporttype"), authDF.col("pcfip"),
              authDF.col("result"), userDF.col("vpdncompanycode"), userDF.col("custprovince"),
              authDF.col("d"), authDF.col("h"))

        } else if (authlogType == AUTH_LOGTYPE_VPDN) {

          authDF = authDF.withColumn("result", when(col("auth_result") === 1, "success").otherwise("failed"))

          resultDF = authDF.join(userDF, userDF.col("mdn") === authDF.col("mdn"), "left").
            select(authDF.col("auth_result"), authDF.col("authtime"), authDF.col("device"), authDF.col("imsicdma"),
              authDF.col("imsilte"), authDF.col("mdn"), authDF.col("nai_sercode"),
              authDF.col("entname"), authDF.col("lnsip"), authDF.col("pdsnip"),
              authDF.col("result"), userDF.col("vpdncompanycode"), userDF.col("custprovince"),
              authDF.col("d"), authDF.col("h"))

        }
        resultDF
      }


      val resultDF = getAuthLogDF(authlogType)


      // 计算cloalesce的数量
      val coalesceNum = makeCoalesce(fileSystem, inputLocation, coalesceSize)
      logInfo(s"$inputPath , $coalesceSize, $coalesceNum")

      // 结果数据分区字段
      val partitions = "d,h"
      // 将数据存入到HDFS， 并刷新分区表
      CommonETLUtils.saveDFtoPartition(sqlContext, fileSystem, resultDF, coalesceNum, partitions, hourid, outputPath + authlogType + "/", authLogDayHourTable, appName)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        logError("[" + appName + "] 失败 处理异常" + e.getMessage)
    }
    finally {
      sc.stop()
    }

  }

}