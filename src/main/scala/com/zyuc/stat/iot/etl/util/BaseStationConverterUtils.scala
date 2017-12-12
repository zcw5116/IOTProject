package com.zyuc.stat.iot.etl.util

import org.apache.spark.Logging
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row


/**
  * Created by zhoucw on 17-7-23.
  */
object BaseStationConverterUtils extends Logging{

  val struct = StructType(Array(
    StructField("enbid", StringType),
    StructField("provId", StringType),
    StructField("provName", StringType),
    StructField("cityId", StringType),
    StructField("cityName", StringType),
    StructField("zhLabel", StringType),
    StructField("userLabel", StringType),
    StructField("vendorId", StringType),
    StructField("vndorName", StringType)
  ))



    def parseLine(line:String) :Row = {
      try {
        val p = line.split("\\|", 63)
        val enbid = p(2)
        val provId = if(null == p(6)) "" else p(6)
        val provName = if(null == p(7)) "" else p(7)
        val cityId = if(null == p(8)) "" else p(8)// regionId, 应该是oracle表数据字段对应错误
        val cityName = if(null == p(9)) "" else p(9) // regionName, 应该是oracle表数据字段对应错误
        val zhLabel = if(null == p(19)) "" else p(19)
        val userLabel = if(null == p(20)) "" else p(20)
        val vendorId = if(null == p(21)) "" else p(21)
        val vndorName = if(null == p(22)) "" else p(22)

        Row(enbid, provId, provName, cityId, cityName, zhLabel, userLabel, vendorId, vndorName)
      }catch {
        case e:Exception => {
          logError("ParseError log[" + line + "] msg[" + e.getMessage + "]")
          Row("0")
        }
      }
    }
}
