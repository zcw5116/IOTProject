package com.zyuc.stat.iot.etl.util

import org.apache.spark.sql.{DataFrame, Row}
import com.alibaba.fastjson.JSON


/**
  * Created by zhoucw on 17-7-23.
  */
object MMEConverterUtils {

  val MME_HWMM_TYPE:String = "hwmm"
  val MME_HWSM_TYPE:String = "hwsm"
  val MME_ZTMM_TYPE:String = "ztmm"
  val MME_ZTSM_TYPE:String = "ztsm"

  def parseMME(mmeDF:DataFrame, mmetype:String) = {
    var newDF:DataFrame = null
    if(mmetype == MME_HWSM_TYPE){
      newDF = mmeDF.selectExpr("T8 as procedureid", "T0 as starttime", "'' as acctype", "T5 as IMSI", "T6 as MSISDN",
        "T14 as sergw", "T13 as pcause", "T7 as imei", "'' as ci", "T12 as eNBID", "T24 as uemme", "'' as newgrpid",
        "'' as newmmecode", "'' as newmtmsi", "'' as oldmcc", "'' as oldgrpid", "'' as oldmmecode", s"'$mmetype' as mmetype",
        s"case when T13='0x0000' then 'success' else 'failed' end as result",
        "'' as oldmtmsi", "T99 as province", "substr(regexp_replace(T0,'-',''),3,6) as d", "substr(T0,12,2) as h", "floor(substr(T0,15,2)/5)*5 as m5")
    }else{
      newDF = mmeDF.selectExpr("T8 as procedureid", "T0 as starttime", "T10 as acctype", "T5 as IMSI", "T6 as MSISDN",
        "T14 as sergw", "T13 as pcause", "T7 as imei", "T43 as ci", "T12 as eNBID", "T24 as uemme",
        "T17 as newgrpid", "T18 as newmmecode", "T19 as newmtmsi", "T28 as oldmcc", "T21 as oldgrpid",
        "T22 as oldmmecode", "T23 as oldmtmsi", "T99 as province", s"'$mmetype' as mmetype",
        s"case when '$mmetype' in ('$MME_HWMM_TYPE', '$MME_HWSM_TYPE') and T13='0x0000' then 'success' when '$mmetype' in ('$MME_ZTMM_TYPE', '$MME_ZTSM_TYPE') and T13='4294967295' then 'success' else 'failed' end as result",
        "substr(regexp_replace(T0,'-',''),3,6) as d", "substr(T0,12,2) as h", "floor(substr(T0,15,2)/5)*5 as m5")
    }
    newDF
  }

}
