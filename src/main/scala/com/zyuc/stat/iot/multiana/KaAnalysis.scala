package com.zyuc.stat.iot.multiana

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by limm on 2017/8/11.
  * 卡分析
  */
object KaAnalysis {
  def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf()
      val sc = new SparkContext(sparkConf)
      val sqlContext = new HiveContext(sc)
      sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

      //参数处理：args(0)为日期,args(1)为日期类型：D:DAY,  H:HOUR:  M:MONTH
      val begintime = args(0)
      val datetype  = args(1)
      val inputPath_4g = "hadoop/IOT/data/cdr/secondaryoutput/pgw/data"
      val inputPath_3g = "/hadoop/IOT/data/cdr/secondaryoutput/pdsn/data"
      val inputPath_user = "/hadoop/IOT/ANALY_PLATFORM/BasicData/output/UserInfo/data"
      val outputPath = "/hadoop/IOT/ANALY_PLATFORM/SummeryDate/output/UserInfo/json/data"
      if(datetype.equals("D")) {
          val resultDF_4G = KaAnalybyDay(sc, sqlContext, begintime, inputPath_4g, "activenum_4g")
          val resultDF_3G = KaAnalybyDay(sc, sqlContext, begintime, inputPath_3g, "activenum_3g")
          // 用户汇总
          val beginD = begintime.substring(0, 8)
          val cachedUserinfoTable = "iot_user_active_cached"
          // val sourceDF = sqlContext.read.format("orc").load(inputPath_user).filter("d="+beginD)

          val cacheUserdDF = sqlContext.sql(
              s""" select distinct custprovince,vpdncompanycode
                 |from iot_customer_userinfo where d ='${beginD}'
           """.stripMargin).cache()
          // 话单与用户信息关联
          val resultDF_user = {
              cacheUserdDF.join(resultDF_4G, Seq("custprovince", "vpdncompanycode"), "left").join(resultDF_3G, Seq("custprovince", "vpdncompanycode"), "left").
                select(cacheUserdDF("custprovince"), cacheUserdDF("vpdncompanycode"), resultDF_4G("activenum_4g").eqNullSafe(0), resultDF_3G("activenum_3g").eqNullSafe(0))
              // 将结果写入json文件中
              //resultDF_user.write.mode(SaveMode.Overwrite).format("json").save(outputPath)
          }
      }else{

      }
    //resultDF_3G.col("custprovince")===filterDF.col("custprovince"),
   // resultDF_3G.col("vpdncompanycode")===filterDF.col("vpdncompanycode"),

       // 释放资源
       sc.stop()
      //按日汇总
      def KaAnalybyDay(sc: SparkContext, sqlContext: HiveContext,analyday:String,inputPath:String,aliasname:String): DataFrame ={

          val beginD = analyday.substring(2,8)
          val sourceDF = sqlContext.read.format("orc").load(inputPath).filter("d="+beginD)
          val filterDF = sourceDF.groupBy(sourceDF.col("custprovince"),sourceDF.col("vpdncompanycode"),sourceDF.col("d")).agg(count(lit(1)).alias(aliasname))
          filterDF
      }
      // 按月汇总
      def KaAnalybyMon(analyMon:String): Unit ={
        val beginM = begintime.substring(2,8)

      }
  }

}
