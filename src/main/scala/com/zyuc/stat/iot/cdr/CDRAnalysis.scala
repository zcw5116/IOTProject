package com.zyuc.stat.iot.cdr

import com.zyuc.stat.iot.service.CDRDataService
import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.DateUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
  * Created by zhoucw on 17-8-7.
  */
object CDRAnalysis {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()//.setAppName("AuthLogAnalysisHbase").setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)
    val starttimeid = sc.getConf.get("spark.app.starttimeid") // 20170801004000
    val partitionD = starttimeid.substring(2, 8)
    val partitionH = starttimeid.substring(8, 10)
    val partitionM5 = starttimeid.substring(10, 12)
    val dayid = starttimeid.substring(0, 8)
    var userTablePartitionID = DateUtils.timeCalcWithFormatConvertSafe(starttimeid.substring(0,8), "yyyyMMdd", -1*24*3600, "yyyyMMdd")
    userTablePartitionID = sc.getConf.get("spark.app.userTablePartitionDayID", userTablePartitionID)
    val userTable = sc.getConf.get("spark.app.userTable") //"iot_customer_userinfo"
    val pgwLocation = sc.getConf.get("spark.app.pgwLocation")    //  "/hadoop/IOT/data/cdr/pgw/output/data/"
    val pdsnLocation = sc.getConf.get("spark.app.pdsnLocation")  //  "/hadoop/IOT/data/cdr/pdsn/output/data/"


    val userDF = sqlContext.table(userTable).filter("d=" + userTablePartitionID).
      selectExpr("mdn", "imsicdma", "custprovince", "case when length(vpdncompanycode)=0 then 'N999999999' else vpdncompanycode end  as vpdncompanycode").
      cache()

    // pdsn flow
    val pdsnSrcDF = sqlContext.read.format("orc").load(pdsnLocation)

    val pdsnDF = pdsnSrcDF.filter("d=" + partitionD).filter("h=" + partitionH).
      filter("m5=" + partitionM5).
      select(pdsnSrcDF.col("mdn"), pdsnSrcDF.col("originating"), pdsnSrcDF.col("termination")).withColumn("type", lit("pdsn")).
      withColumn("upflow", when(length(pdsnSrcDF.col("originating"))===lit(0), 0).otherwise(pdsnSrcDF.col("originating"))).
      withColumn("downflow", when(length(pdsnSrcDF.col("termination"))===lit(0), 0).otherwise(pdsnSrcDF.col("termination")))

    val pdsnAggDF = pdsnDF.join(userDF, pdsnDF.col("mdn")===userDF.col("mdn")).groupBy(userDF.col("vpdncompanycode"),
      pdsnDF.col("type")).agg(sum(pdsnDF.col("upflow")).alias("upflow"),
      sum(pdsnDF.col("downflow")).alias("downflow"))


    // pgw flow
    val pgwSrcDF = sqlContext.read.format("orc").load(pgwLocation)

    val pgwDF = pgwSrcDF.filter("d=" + partitionD).filter("h=" + partitionH).
      filter("m5=" + partitionM5).
      select(pgwSrcDF.col("mdn"), pgwSrcDF.col("l_datavolumefbcuplink"),
        pgwSrcDF.col("l_datavolumefbcdownlink")).withColumn("type", lit("pgw")).
      withColumn("upflow", when(pgwSrcDF.col("l_datavolumefbcuplink").isNull, 0).otherwise(pgwSrcDF.col("l_datavolumefbcuplink"))).
      withColumn("downflow", when(pgwSrcDF.col("l_datavolumefbcdownlink").isNull, 0).otherwise(pgwSrcDF.col("l_datavolumefbcdownlink")))

    val pgwAggDF = pgwDF.join(userDF, pgwDF.col("mdn")===userDF.col("mdn")).groupBy(userDF.col("vpdncompanycode"),
      pgwDF.col("type")).agg(sum(pgwDF.col("upflow")).alias("upflow"),
      sum(pgwDF.col("downflow")).alias("downflow"))


    val cdrDF = pdsnAggDF.unionAll(pgwAggDF).coalesce(1)


    CDRDataService.saveRddData(cdrDF, starttimeid)

    import sqlContext.implicits._
    val hbaseDF = CDRDataService.registerCdrRDD(sc, starttimeid).toDF()

    CDRDataService.SaveRddToAlarm(sc, sqlContext, hbaseDF, starttimeid)


    sc.stop()

  }

}
