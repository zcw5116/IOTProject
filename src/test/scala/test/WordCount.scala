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

    val intervalDay = 3

    for(i <- 1 to intervalDay){
           println(i)
    }

  }

}