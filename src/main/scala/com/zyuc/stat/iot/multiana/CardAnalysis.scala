package com.zyuc.stat.iot.multiana


import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by zhoucw on 17-8-17.
  */
object CardAnalysis {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setAppName("OperalogAnalysis").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val appName =  sc.getConf.get("spark.app.name")
    val userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionID")
    val userTable = sc.getConf.get("spark.app.table.userTable") //"iot_customer_userinfo"
    val operTable = sc.getConf.get("spark.app.table.iot_operlog_data_day") //"iot_operlog_data_day"
    val onlineTable = sc.getConf.get("spark.app.table.onlineTable") //"iot_analy_online_day"
    val activedUserTable = sc.getConf.get("spark.app.table.activedUserTable") //"iot_activeduser_data_day"
    val dayid = sc.getConf.get("spark.app.dayid")
    val outputPath = sc.getConf.get("spark.app.outputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/card/
    val localOutputPath =  sc.getConf.get("spark.app.localOutputPath") // /slview/test/zcw/shell/card/json/
    val partitionD = dayid.substring(2, 8)


    val tmpCompanyTable = s"${appName}_tmp_Company"
    sqlContext.sql(
      s"""select distinct (case when length(custprovince)=0 or custprovince is null then '其他' else custprovince end)  as custprovince,
         |       case when length(vpdncompanycode)=0 or vpdncompanycode is null then 'N999999999' else vpdncompanycode end  as vpdncompanycode
         |from ${userTable}
         |where d='${userTablePartitionID}'
       """.stripMargin).cache().registerTempTable(tmpCompanyTable)

    val tmpCompanyNetTable = s"${appName}_tmp_CompanyNet"
    val companyDF = sqlContext.sql(
      s"""select custprovince, vpdncompanycode, '2/3G' as nettype from ${tmpCompanyTable}
         |union all
         |select custprovince, vpdncompanycode, '4G' as nettype from ${tmpCompanyTable}
         |union all
         |select custprovince, vpdncompanycode, '2/3/4G' as nettype from ${tmpCompanyTable}
       """.stripMargin
    ).cache()

    // Operlog
    val operDF = sqlContext.table(operTable).filter("d="+partitionD)
    var resultDF = companyDF.join(operDF, companyDF.col("vpdncompanycode")===operDF.col("vpdncompanycode") &&
      companyDF.col("nettype")===operDF.col("nettype"), "left").select(companyDF.col("custprovince"),
      companyDF.col("vpdncompanycode"), companyDF.col("nettype"), operDF.col("opensum"),  operDF.col("closesum"))


    // online
    val onlineDF = sqlContext.table(onlineTable).filter("d="+partitionD).selectExpr("vpdncompanycode",
    "case when nettype='3G' then '2/3G' else nettype end as nettype", "onlinenum")

    resultDF =  resultDF.join(onlineDF, resultDF.col("vpdncompanycode")===onlineDF.col("vpdncompanycode") &&
      resultDF.col("nettype")===onlineDF.col("nettype"), "left").select(resultDF.col("custprovince"),
      resultDF.col("vpdncompanycode"), resultDF.col("nettype"), resultDF.col("opensum"),
      resultDF.col("closesum"),onlineDF.col("onlinenum"))

    // active
    val activedUserDF = sqlContext.table(activedUserTable).filter("d="+partitionD).selectExpr("vpdncompanycode",
      "case when nettype='3G' then '2/3G' else nettype end as nettype", "activednum")

    resultDF =  resultDF.join(activedUserDF, resultDF.col("vpdncompanycode")===activedUserDF.col("vpdncompanycode") &&
      resultDF.col("nettype")===activedUserDF.col("nettype"), "left").
      select(
      resultDF.col("custprovince"), resultDF.col("vpdncompanycode"), resultDF.col("nettype"),
        when(resultDF.col("opensum").isNull, 0).otherwise(resultDF.col("opensum")).alias("opensum"),
        when(resultDF.col("closesum").isNull, 0).otherwise(resultDF.col("closesum")).alias("closesum"),
        when(resultDF.col("onlinenum").isNull,0).otherwise(resultDF.col("onlinenum")).alias("onlinenum"),
        when(activedUserDF.col("activednum").isNull,0).otherwise(activedUserDF.col("activednum")).alias("activednum")).
      withColumn("dayid", lit(dayid))


    val coalesceNum = 1
    val outputLocatoin = outputPath + "json/data/" + dayid
    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(outputLocatoin)

    FileUtils.downFilesToLocal(fileSystem, outputLocatoin, localOutputPath, dayid, ".json")

  }

}
