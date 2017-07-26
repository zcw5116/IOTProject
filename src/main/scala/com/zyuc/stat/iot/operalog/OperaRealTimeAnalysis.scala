package com.zyuc.stat.iot.operalog

import com.zyuc.stat.properties.ConfigProperties
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import com.zyuc.stat.utils.HbaseUtils._

/**
  * Created by slview on 17-6-17.
  */
object OperaRealTimeAnalysis {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <dayid>")
      System.exit(1)
    }
    val dayid = args(0)
    val sparkConf = new SparkConf().setAppName("OperalogAnalysis")//.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use iot")
    val prcftable = "iot_user_opera_pcrf_log"
    val hsstable = "iot_user_opera_hss_log"
    val hlrtable = "iot_user_opera_hlr_log"
    val operaresultTable = "iot_user_opera_result"
    val monthid = dayid.substring(0,6)
    val cachedUserinfoTable = "iot_user_basic_info_cached"
    sqlContext.sql("CACHE LAZY TABLE " + cachedUserinfoTable + "  as select u.mdn, " +
      "case when length(u.vpdncompanycode)=0 then 'N999999999' else u.vpdncompanycode end  as vpdncompanycode " +
      "from iot_user_basic_info u ").coalesce(1)

    // sqlContext.sql("insert into " + operaresultTable + "  partition(monthid=" + monthid + ")  " +
    val operaDF = sqlContext.sql( " select 'pcrf' as operatype, u.vpdncompanycode, (case when l.opertype='开户' then 'open' else 'close' end) as  opertype, count(*) as operacnt " +
      " from " + prcftable + " l, " + cachedUserinfoTable + " u " +
      " where l.opertype in('开户','销户')  and l.oper_result='成功'  and  l.mdn = u.mdn and l.dayid='"+dayid+"'  " +
      "  group by  u.vpdncompanycode,l.opertype " +
      "union all" +
      " select 'hss' as operatype, u.vpdncompanycode, (case when l.opertype='开户' then 'open' else 'close' end) as  opertype, count(*) as operacnt " +
      " from " + hsstable + " l, " + cachedUserinfoTable + " u " +
      " where l.opertype in('开户','销户')  and l.oper_result='成功'  and  l.mdn = u.mdn and l.dayid='"+dayid+"' " +
      "  group by  u.vpdncompanycode,l.opertype " +
      "union all" +
      " select 'hlr' as operatype, u.vpdncompanycode, (case when l.opertype='开户' then 'open' else 'close' end) as  opertype, count(*) as operacnt " +
      " from " + hlrtable + " l, " + cachedUserinfoTable + " u " +
      " where l.opertype in('开户','销户')  and l.oper_result='成功'  and  l.mdn = u.mdn  and l.dayid='"+dayid+"'  " +
      "  group by  u.vpdncompanycode,l.opertype "
    ).coalesce(1)
    // operaDF.registerTempTable("test111")

    val tableName = "iot_operalog_day" // + dayid

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","EPC-LOG-NM-15,EPC-LOG-NM-17,EPC-LOG-NM-16")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    conf.set("hbase.zookeeper.property.clientPort", ConfigProperties.IOT_ZOOKEEPER_CLIENTPORT)
    conf.set("hbase.zookeeper.quorum", ConfigProperties.IOT_ZOOKEEPER_QUORUM)
    val connection= ConnectionFactory.createConnection(conf)
    val families = new Array[String](1)
    families(0) = "operainfo"
    // 创建表, 如果表存在， 自动忽略
    createIfNotExists(tableName,families)

    val operaJobConf = new JobConf(conf, this.getClass)
    operaJobConf.setOutputFormat(classOf[TableOutputFormat])
    operaJobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // type, vpdncompanycode, authcnt, successcnt, failedcnt, authmdnct, authfaieldcnt
    val operaRDD = operaDF.rdd.map(x => (x.getString(0), x.getString(1), x.getString(2), x.getLong(3)))

    val operaHbaseRdd = operaRDD.map { arr => {
        val currentPut = new Put(Bytes.toBytes(arr._2 + "-" + dayid.toString))
        currentPut.addColumn(Bytes.toBytes("operainfo"), Bytes.toBytes(arr._1 + "_"+arr._3+"_cnt"), Bytes.toBytes(arr._4.toString))
        //currentPut.addColumn(Bytes.toBytes("operainfo"), Bytes.toBytes(arr._1 + "_"+arr._3+"_cnt"), Bytes.toBytes(arr._4.toString))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, currentPut)
    }
    }
    operaHbaseRdd.saveAsHadoopDataset(operaJobConf)

    sc.stop()
  }

}
