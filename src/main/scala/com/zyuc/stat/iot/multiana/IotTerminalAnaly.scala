package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by LiJian on 2017/7/28.
  */
object IotTerminalAnaly extends Logging{
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()//.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val userTable = sc.getConf.get("spark.app.user.tableName")     // "iot_customer_userinfo"
    val userTablePartitionDayid = sc.getConf.get("spark.app.user.userTablePartitionDayid")  //  "20170801"
    val terminalInputPath = sc.getConf.get("spark.app.terminalInputPath")  // /hadoop/IOT/data/terminal/output/data/
    val terminalOutputPath = sc.getConf.get("spark.app.terminalOutputPath")  // /hadoop/IOT/data/terminal/output/
    val localOutputPath = sc.getConf.get("spark.app.localOutputPath")  // /slview/test/zcw/shell/terminal/json/
    val terminalResultTable = sc.getConf.get("spark.app.terminalResultTable")
    //val companyInfoTable = sc.getConf.get("spark.app.table.companyInfo") //"iot_activeduser_data_day"
    val appName = sc.getConf.get("spark.app.name") // terminalmultiAnalysis_2017073111
    val coalesceNum = sc.getConf.get("spark.app.coalesceNum", "1")

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)

    val terminalLocation = terminalInputPath + "/data"
    val terminalDF = sqlContext.read.format("orc").load(terminalLocation)


    val userDF = sqlContext.table(userTable).filter("d=" + userTablePartitionDayid).
      selectExpr("mdn", "imei", "case when length(custprovince)=0 or custprovince is null then '其他' else custprovince end  as custprovince", "case when length(vpdncompanycode)=0 then 'N999999999' else vpdncompanycode end  as vpdncompanycode").
      cache()

    val aggTmpDF = userDF.join(terminalDF, userDF.col("imei").substr(0,8)===terminalDF.col("tac"), "left").
      groupBy(userDF.col("custprovince"), userDF.col("vpdncompanycode"), terminalDF.col("devicetype"),
        terminalDF.col("modelname")).agg(count(lit(1)).alias("tercnt"))


    val  aggDF = aggTmpDF


    // 处理空值
    val resultDF = aggDF.select(lit(userTablePartitionDayid).alias("datetime"),
      when(aggDF.col("custprovince").isNull, "其他").otherwise(aggDF.col("custprovince")).alias("custprovince"),
      when(aggDF.col("vpdncompanycode").isNull, "N999999999").otherwise(aggDF.col("vpdncompanycode")).alias("companycode"),
      when(aggDF.col("devicetype").isNull, "").otherwise(aggDF.col("devicetype")).alias("devicetype"),
      when(aggDF.col("modelname").isNull, "").otherwise(aggDF.col("modelname")).alias("modelname"),
      aggDF.col("tercnt"))


    val terminalOutputLocatoin = terminalOutputPath + "json/data/" + userTablePartitionDayid

    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(terminalOutputLocatoin)

    FileUtils.downFilesToLocal(fileSystem, terminalOutputLocatoin, localOutputPath, userTablePartitionDayid, ".json")

  }
}
