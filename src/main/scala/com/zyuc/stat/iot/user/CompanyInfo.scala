package com.zyuc.stat.iot.user

import com.zyuc.stat.properties.ConfigProperties
import com.zyuc.stat.utils.{DateUtils, HbaseUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhoucw on 17-8-29.
  */
object CompanyInfo extends Logging{

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //.setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)

    val userTablePartitionID = sc.getConf.get("spark.app.table.userTablePartitionDayID")
    val userTable = sc.getConf.get("spark.app.table.userTable") //"iot_customer_userinfo"
    val appName = sc.getConf.get("spark.app.name")
    val nextWeekDayid = DateUtils.timeCalcWithFormatConvertSafe(userTablePartitionID, "yyyyMMdd", 7*24*3600, "yyyyMMdd")


    val tmpUserTable = appName + "_usertable_" +  userTablePartitionID
    val userSQL =
      s"""
         |select mdn, imsicdma, custprovince, case when length(vpdncompanycode)=0 then 'N999999999' else vpdncompanycode end  as companycode
         |from ${userTable} where d=${userTablePartitionID}
       """.stripMargin
    logInfo("userSQL: " + userSQL)
    sqlContext.sql(userSQL).registerTempTable(tmpUserTable)

    val resultDF = sqlContext.sql(s"select companycode, count(*) as usernum from $tmpUserTable group by  companycode").coalesce(1)

    val htable = "iot_basic_companyinfo"
    // 如果h表不存在， 就创建
    // hbase配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    val connection = ConnectionFactory.createConnection(conf)
    val families = new Array[String](1)
    families(0) = "companyinfo"
    // 创建表, 如果表存在， 自动忽略
    HbaseUtils.createIfNotExists(htable, families)
    val companyJobConf = new JobConf(conf, this.getClass)
    companyJobConf.setOutputFormat(classOf[TableOutputFormat])
    companyJobConf.set(TableOutputFormat.OUTPUT_TABLE, htable)

    // 预警表
    val alarmChkTable = "analyze_rst_tab"
    val alarmChkJobConf = new JobConf(conf, this.getClass)
    alarmChkJobConf.setOutputFormat(classOf[TableOutputFormat])
    alarmChkJobConf.set(TableOutputFormat.OUTPUT_TABLE, alarmChkTable)

    val companyHbaseRDD = resultDF.rdd.map(x => (x.getString(0), x.getLong(1)))
    val companyRDD = companyHbaseRDD.map { arr => {
      val curPut = new Put(Bytes.toBytes(arr._1 + "_" + userTablePartitionID.toString))
      val nextWeekPut = new Put(Bytes.toBytes(arr._1 + "_" + nextWeekDayid))
      val alarmPut = new Put(Bytes.toBytes(arr._1))

      curPut.addColumn(Bytes.toBytes("companyinfo"), Bytes.toBytes("usernum"), Bytes.toBytes(arr._2.toString))
      nextWeekPut.addColumn(Bytes.toBytes("companyinfo"), Bytes.toBytes("lastweek_usernum"), Bytes.toBytes(arr._2.toString))

      alarmPut.addColumn(Bytes.toBytes("alarmChk"), Bytes.toBytes("usernum"), Bytes.toBytes(arr._2.toString))

      ((new ImmutableBytesWritable, curPut),(new ImmutableBytesWritable, nextWeekPut),(new ImmutableBytesWritable, alarmPut))
    }
    }
    companyRDD.map(_._1).saveAsHadoopDataset(companyJobConf)
    // companyRDD.map(_._2).saveAsHadoopDataset(companyJobConf)
    companyRDD.map(_._3).saveAsHadoopDataset(alarmChkJobConf)

  }

}
