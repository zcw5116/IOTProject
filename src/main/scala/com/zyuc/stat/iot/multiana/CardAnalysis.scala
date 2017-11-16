package com.zyuc.stat.iot.multiana


import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

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
    val userTable = sc.getConf.get("spark.app.table.userTable","iot_basic_userinfo") //"iot_customer_userinfo" => 'iot_basic_userinfo'
    val operTable = sc.getConf.get("spark.app.table.operaLogTable") //"iot_operlog_data_day"
    val onlineTable = sc.getConf.get("spark.app.table.onlineTable","iot_useronline_base_nums") //"iot_analy_online_day"===> iot_useronline_base_nums
    val activedUserTable = sc.getConf.get("spark.app.table.activedUserTable") //"iot_activeduser_data_day"
    //val companyInfoTable = sc.getConf.get("spark.app.table.companyInfo") //"iot_activeduser_data_day"
    val dayid = sc.getConf.get("spark.app.dayid")
    val outputPath = sc.getConf.get("spark.app.outputPath") // hdfs://EPC-IOT-ES-06:8020/hadoop/IOT/data/multiAna/card/
    val localOutputPath =  sc.getConf.get("spark.app.localOutputPath") // /slview/test/zcw/shell/card/json/
    val partitionD = dayid.substring(2, 8)


    val tmpCompanyTable = s"${appName}_tmp_Company"
    sqlContext.sql(
      s"""select distinct (case when length(belo_prov)=0 or belo_prov is null then '其他' else belo_prov end)  as custprovince,
         |           case when length(companycode)=0 or companycode is null then 'P999999999' else companycode end  as vpdncompanycode
         |    from ${userTable}
         |    where d='${userTablePartitionID}'
         |
       """.stripMargin).cache().registerTempTable(tmpCompanyTable)

    val tmpCompanyNetTable = s"${appName}_tmp_CompanyNet"
    val companyDF = sqlContext.sql(
      s"""select custprovince, vpdncompanycode,  '2/3G' as nettype from ${tmpCompanyTable}
         |union all
         |select custprovince, vpdncompanycode,  '4G' as nettype from ${tmpCompanyTable}
         |union all
         |select custprovince, vpdncompanycode,  '2/3/4G' as nettype from ${tmpCompanyTable}
       """.stripMargin
    ).cache()


    // Operlog
    val operDF = sqlContext.table(operTable).filter("d="+partitionD)
    var resultDF = companyDF.join(operDF, companyDF.col("vpdncompanycode")===operDF.col("vpdncompanycode") &&
      companyDF.col("nettype")===operDF.col("nettype"), "left").select(companyDF.col("custprovince"),
      companyDF.col("vpdncompanycode"),  companyDF.col("nettype"),
      operDF.col("opennum"),  operDF.col("closenum"))


    // online
    //val onlineHourDF = sqlContext.table(onlineTable).filter("d="+partitionD).selectExpr("vpdncompanycode",
    //"case when nettype='3G' then '2/3G' else nettype end as nettype", "floor(onlinenum)")
   //// val onlineHourDF = sqlContext.sql(
   ////   s"""select vpdncompanycode, case when nettype='3G' then '2/3G' else nettype end as nettype,
   ////      |      floor(round(avg(onlinenum),0))  as onlinenum
   ////      |from ${onlineTable}
   ////      |group by vpdncompanycode, case when nettype='3G' then '2/3G' else nettype end
   ////    """.stripMargin
   //// ).cache()
    val onlineHourDF = sqlContext.sql(
      s"""select vpdncompanycode,floor(avg(g3cnt)) onlinenum,'2/3G' as nettype
         |from  ${onlineTable}
         |where d = "${partitionD}"
         |group by vpdncompanycode,'2/3G'
         |union all
         |select vpdncompanycode,floor(avg(pgwcnt)) onlinenum,'4G' as nettype
         |from  ${onlineTable}
         |where d = "${partitionD}"
         |group by vpdncompanycode,'4G'
       """.stripMargin
    )

    //val onlineDF = onlineHourDF.groupBy(onlineHourDF.col("vpdncompanycode"), onlineHourDF.col("nettype")).
    //agg(floor(avg(onlineHourDF.col("onlinenum"))).alias("onlinenum"))



    //resultDF =  resultDF.join(onlineDF, resultDF.col("vpdncompanycode")===onlineDF.col("vpdncompanycode") &&
    //  resultDF.col("nettype")===onlineDF.col("nettype"), "left").select(resultDF.col("custprovince"),
    //  resultDF.col("vpdncompanycode"),  resultDF.col("nettype"), resultDF.col("opennum"),
    //  resultDF.col("closenum"),onlineDF.col("onlinenum"))

    resultDF =  resultDF.join(onlineHourDF, resultDF.col("vpdncompanycode")===onlineHourDF.col("vpdncompanycode") &&
      resultDF.col("nettype")===onlineHourDF.col("nettype"), "left").select(resultDF.col("custprovince"),
      resultDF.col("vpdncompanycode"),  resultDF.col("nettype"), resultDF.col("opennum"),
      resultDF.col("closenum"),onlineHourDF.col("onlinenum"))

    // active
    val activedUserDF = sqlContext.table(activedUserTable).filter("d="+partitionD).selectExpr("vpdncompanycode",
      "case when nettype='3G' then '2/3G' else nettype end as nettype", "activednum")

    resultDF =  resultDF.join(activedUserDF, resultDF.col("vpdncompanycode")===activedUserDF.col("vpdncompanycode") &&
      resultDF.col("nettype")===activedUserDF.col("nettype"), "left").
      select(
      resultDF.col("custprovince"), resultDF.col("vpdncompanycode").alias("companycode"),
        resultDF.col("nettype"),
        when(resultDF.col("opennum").isNull, 0).otherwise(resultDF.col("opennum")).alias("opennum"),
        when(resultDF.col("closenum").isNull, 0).otherwise(resultDF.col("closenum")).alias("closenum"),
        when(resultDF.col("onlinenum").isNull,0).otherwise(resultDF.col("onlinenum")).alias("onlinenum"),
        when(activedUserDF.col("activednum").isNull,0).otherwise(activedUserDF.col("activednum")).alias("activednum")).
      withColumn("datetime", lit(dayid))


    val coalesceNum = 1
    val outputLocatoin = outputPath + "tmp/" + dayid + "/"
    val fileSystem = FileSystem.newInstance(sc.hadoopConfiguration)

    resultDF.repartition(coalesceNum.toInt).write.mode(SaveMode.Overwrite).format("json").save(outputLocatoin)
    FileUtils.moveTempFilesToESpath(fileSystem,outputPath,dayid,dayid)
    //FileUtils.downFilesToLocal(fileSystem, outputLocatoin, localOutputPath, dayid, ".json")

  }

}
