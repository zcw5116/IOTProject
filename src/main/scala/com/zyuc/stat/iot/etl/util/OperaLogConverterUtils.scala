package com.zyuc.stat.iot.etl.util

import org.apache.spark.sql.DataFrame


/**
  * Created by zhoucw on 17-7-23.
  */
object OperaLogConverterUtils {
  val Oper_PCRF_PLATFORM:String = "PCRF"
  val Oper_HSS_PLATFORM:String = "HSS"
  val Oper_HLR_PLATFORM:String = "HLR"

  def parse(df: DataFrame, platformName:String) = {
    df.selectExpr(s"'$platformName' as platform ", "detailinfo", "errorinfo", "imsicdma", "imsilte", "mdn", "netype", "node",
      "operclass", "opertime", "case when opertype='开户' then 'install' when when opertype='销户' then 'remove' else 'else' end as opertype", "oper_result", "substr(regexp_replace(opertime,'-',''),1,8) as d")
  }
}
