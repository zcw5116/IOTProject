package com.zyuc.stat.iot.etl.util


import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by zhoucw on 17-8-18.
  */
object CompanyConverterUtils extends Logging{

  var struct = StructType(Array(
    StructField("companyCode", StringType),
    StructField("companyName", StringType),
    StructField("vpdn", StringType, false),
    StructField("apn", StringType, false)
  ))


  def parseLine(line:String) :Row = {
    try {
      val p = line.split("\\|", 4)
      if(p(0)=="C000001661"){
        println(line)
      }
      Row(p(0), p(1), p(2), p(3))
    }catch {
      case e:Exception => {
        logError("ParseError log[" + line + "] msg[" + e.getMessage + "]")
        Row("0")
      }
    }


  }
}
