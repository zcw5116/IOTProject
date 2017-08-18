package com.zyuc.stat.iot.etl.util


import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by zhoucw on 17-7-27.
  *   case class UserInfo(mdn:String, imsicdma:String, imsilte:String, iccid:String, imei:String, company:String,
  *   vpdncompanycode:String, nettype:String, vpdndomain:String, isvpdn:String, subscribetimeaaa:String, subscribetimehlr:String,
  *   subscribetimehss:String, subscribetimepcrf:String, firstactivetime:String, userstatus:String, atrbprovince:String, userprovince:String)

  */
object UserInfoConverterUtils extends Logging{
  val struct = StructType(Array(
    StructField("mdn", StringType),
    StructField("imsicdma", StringType),
    StructField("imsilte", StringType),
    StructField("iccid", StringType),
    StructField("imei", StringType),

    StructField("company", StringType),
    StructField("vpdncompanycode", StringType),
    StructField("nettype", StringType),
    StructField("vpdndomain", StringType),
    StructField("isvpdn", StringType),

    StructField("subscribetimeaaa", StringType),
    StructField("subscribetimehlr", StringType),
    StructField("subscribetimehss", StringType),
    StructField("subscribetimepcrf", StringType),
    StructField("firstactivetime", StringType),

    StructField("userstatus", StringType),
    StructField("atrbprovince", StringType),
    StructField("userprovince", StringType)
  ))


  def parseLine(line:String) = {

    try {
      val p = line.split("\\|",18)
      Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17))
    }catch {
      case e: Exception =>
        logError("ParseError log[" + line + "] msg[" + e.getMessage + "]")
        Row("0")
    }
  }
}
