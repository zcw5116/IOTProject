package com.zyuc.stat.iot.user

import com.zyuc.stat.utils.SparkKafkaUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
 * Created by cuihs on 2017/6/20.
 */

case class pgwradius_out(APN: String, Duration: String, IPAddr: String, MDN: String,
                         InputOctets: String, OutputOctets: String, NetType: String, SessionID: String,
                         Time: String, Status: String, TerminateCause: String, dayid: String)

object IotSourceKafkaDeal {
  def main(args: Array[String]) {
    // 创建StreamingContext
    // 创建上下文
    val sparkConf = new SparkConf()
      .setAppName("IotSourceKafkaDeal")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(300))
    // 创建checkpoint
    //  ssc.checkpoint("/user/slview/checkpoint/IotSourceKafkaDeal")
    // 创建stream时使用的topic名字集合
    val topics: Set[String] = Set("pgwradius_out")
    // zookeeper的host和ip,创建一个client
    val zkClient = new ZkClient("10.37.7.139:2181,10.37.7.140:2181,10.37.7.141:2181")
    // 配置信息
    val brokers = "10.37.7.139:9092,10.37.7.140:9092,10.37.7.141:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    // 获取topic和partition参数
    val groupName = "IotSourceKafkaDeal"
    // 获取kafkaStream
    val kafkaStream = SparkKafkaUtils.createDirectKafkaStream(ssc, kafkaParams, zkClient, topics, groupName)

    // 创建hiveContext
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    //设置参数开启 动态分区（dynamic partition）
    hiveContext.sql("set hive.exec.dynamic.partition.mode = nonstrict")
    hiveContext.sql("set hive.exec.dynamic.partition = true")
    kafkaStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.mapPartitions( partition => {
          val data = partition.map(x => {
            val json = JSON.parseFull(x._2)

            json match {
              // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
              case Some(map: Map[String, String]) => {
                val Status = getMapData(map, "Status")
                val Time =
                  if (Status == "Start") {
                    println(getMapData(map, "StartTime"))
                    parseTime(getMapData(map, "StartTime"))
                  } else if (Status == "Stop") {
                    println(getMapData(map, "StopTime"))
                    parseTime(getMapData(map, "StopTime"))
                  }
                  else
                    "-1"

                val dayid = if (Time.length >= 8) Time.substring(0, 8) else "-1"

                pgwradius_out(getMapData(map, "APN"), getMapData(map, "Duration"),
                  getMapData(map, "IPAddr"), getMapData(map, "MDN"),
                  getMapData(map, "InputOctets"), getMapData(map, "OutputOctets"),
                  getMapData(map, "NetType"), getMapData(map, "SessionID"),
                  Time,
                  getMapData(map, "Status"), getMapData(map, "TerminateCause"),
                  dayid)
              }
              case other => null
            }
          })
          data
        })
          .filter(_ != null)
          //      .coalesce(1)
          .toDF()
          .registerTempTable("registerTempTable_pgwradius_out")

        hiveContext.sql("insert into iot.pgwradius_out partition(dayid) " +
          "select " +
          " APN, " +
          " Duration, " +
          " IPAddr, " +
          " MDN, " +
          " InputOctets, " +
          " OutputOctets, " +
          " NetType, " +
          " SessionID, " +
          " Time, " +
          " Status, " +
          " TerminateCause, " +
          " dayid " +
          "from registerTempTable_pgwradius_out")

        SparkKafkaUtils.saveOffsets(zkClient, groupName, rdd)
      }
    }
    println("streaming start")
    ssc.start()
    ssc.awaitTermination()
  }

  def getMapData(map: Map[String, String], key: String): String = {
    if (map.contains(key))
      map(key)
    else
      "-1"
  }

  def parseTime(timevale: String): String = {
    val timeType = "([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})".r

    timevale match {
      case timeType(a, b, c, d, e, f) => return a + b + c + d + e + f
      case other => return "-1"
    }
  }
}
