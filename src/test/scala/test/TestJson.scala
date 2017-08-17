package test

import com.zyuc.stat.properties.ConfigProperties
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
/**
  * Created by zhoucw on 17-8-13.
  */
object TestJson {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("fd")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use " + ConfigProperties.IOT_HIVE_DATABASE)


/*
    val inputFile = "/hadoop/IOT/data/auth/auth3g/srcdata/201708071205_doing/3gaaa_132.228.105.196_auth_201707010023.log"
    val srcDF = sqlContext.read.format("json").load(inputFile)
    srcDF.show

     srcDF.write.mode(SaveMode.Overwrite).format("orc").save("/tmp/zcw")
*/


    val df = sqlContext.read.format("orc").load("/tmp/zcw/*orc")
    val newDF = df.select(df.col("IMSICDMA"),df.col("mdn"), when(df.col("IMSICDMA").startsWith("4600376"),"1234").otherwise("").alias("ttt"))
    newDF.show()
    newDF.write.format("json").mode(SaveMode.Overwrite).save("/tmp/zcw1/")

    sc.stop()
  }

}
