package spark
import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    var logFile ="E:\\util\\spark_util\\src\\main\\resources\\a.txt";
    var conf = new SparkConf().setAppName("simpleapp").setMaster("local[2]")
    val sc= new SparkContext(conf)
    var logdata = sc.textFile(logFile,2).cache()
    val num = logdata.flatMap(x => x.split(" ")).filter(_.contains("a")).count()
    println(num)
    sc.stop()
  }
}
