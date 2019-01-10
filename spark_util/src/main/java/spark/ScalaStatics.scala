package spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ScalaStatics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("scalastatics").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    val lines = ssc.socketTextStream("10.20.81.14",8080,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.map(_.split(",")).filter(_.length == 6)
    val wordCounts = words.map(x => (1,x(5).toDouble)).reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
