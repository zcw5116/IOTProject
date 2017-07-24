package test

import com.zyuc.stat.utils.DateUtils.timeCalcWithFormatConvertSafe
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by slview on 17-7-11.
  */
object WordCount {
  def main (args: Array[String]) {
/*    val sparkConf = new SparkConf().setAppName("TestWordCount").setMaster("local[4]")

    val ssc = new StreamingContext(sparkConf, Seconds(20))

    val lines = ssc.textFileStream("file:///home/slview/data/test/word/")
    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()*/

    val m5timeid = "201707241520"

    val dayid = m5timeid.substring(2,8)
    val hourid = m5timeid.substring(8,10)
    val m5id = m5timeid.substring(10,12)

    println(dayid)
    println(hourid)
    println(m5id)

  }

}