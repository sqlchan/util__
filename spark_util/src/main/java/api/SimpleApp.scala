package api

import org.apache.spark.{SparkConf, SparkContext}


object SimpleApp {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("simpleapp").setMaster("local[2]")
    val sc= new SparkContext(conf)
    var logFile ="E:\\util\\spark_util\\src\\main\\resources\\a.txt";

//    val part = sc.textFile("E:\\util\\spark_util\\src\\main\\resources\\a.txt")
//    println(part.partitions.size)
    //=====================
//    val part = sc.textFile("E:\\util\\spark_util\\src\\main\\resources\\a.txt",6)
//    println(part.partitions.size)
    //=====================
//    val rdd = sc.textFile("E:\\util\\spark_util\\src\\main\\resources\\a.txt")
//    val wordmap = rdd.flatMap(_.split(",")).map(x => (x,1))
//    println(wordmap.collect())
//    wordmap.dependencies.foreach{dep =>
//      println("dependency type: "+dep.getClass)
//      println("dependenty rdd: "+dep.rdd)
//      println("dependenty partitions: "+dep.rdd.partitions)
//      println("dependenty partitions length: "+dep.rdd.partitions.length)
//    }
    //=========================
//    val rdd = sc.textFile("E:\\util\\spark_util\\src\\main\\resources\\a.txt")
//    rdd.map(line => line.split("\\s+")).collect()
//    rdd.flatMap(line => line.split("\\s+")).collect()
//    rdd.flatMap(line => line.split("\\s+")).distinct().collect()
    //========================
//    val rdd = sc.textFile("E:\\util\\spark_util\\src\\main\\resources\\a.txt")
//    rdd.coalesce(1)
//    rdd.repartition(4)
    //==========================
//    val rdd = sc.textFile("E:\\util\\spark_util\\src\\main\\resources\\a.txt")
//    rdd.randomSplit(Array(1.0,2.0,3.0,4.0))
//    rdd.glom().collect()
    //============================
//    val rdd = sc.textFile("E:\\util\\spark_util\\src\\main\\resources\\a.txt")
//    val rdd1 = sc.textFile("E:\\util\\spark_util\\src\\main\\resources\\a.txt")
//    rdd.union(rdd1)
//    rdd.intersection(rdd1)
//    rdd.subtract(rdd1)
    //==============================
//    val rdd = sc.textFile("E:\\util\\spark_util\\src\\main\\resources\\a.txt")
//    var rdd2 = rdd.mapPartitions{ x => {
//        var res = List[Int]()
//        var i=0
//        while (x.hasNext){
//          i += x.next()
//        }
//        res.::(i).iterator
//      }
//    }
    //========================
//    val rdd = sc.textFile("E:\\util\\spark_util\\src\\main\\resources\\a.txt")
//    val rdd1 = sc.textFile("E:\\util\\spark_util\\src\\main\\resources\\a.txt")
//    rdd.zip(rdd1)
    //======================
//    var rdd = sc.makeRDD(Array(("A",1),("A",2),("B",3),("B",4),("B",5)))
//    var rdd1 = rdd.combineByKey(
//      (v : Int) => ""+ v + "_",
//      (c : String, v : Int) => c+"@"+v,
//      (c1 : String, c2 : String) => c1 +"$"+c2
//    )
//    println(rdd1.first())  // (B,3_@4@5)
    //=====================
//    var rdd = sc.makeRDD(Array(("A",1),("A",2),("B",3),("B",4),("B",5)))
//    var rdd1 = rdd.foldByKey(0)(_+_)
//    println(rdd1.first())
    //===================
//    var rdd = sc.makeRDD(Array(("A",1),("A",2),("B",3),("B",4),("B",5)))
//    rdd.groupByKey()
//    rdd.reduceByKey((x,y) => x+y )
    //==================
//    var rdd1 = sc.makeRDD(Array(("A",1),("A",2),("B",3),("B",4),("B",5)))
//    var rdd2 = sc.makeRDD(Array(("A",1),("A",2),("B",3),("B",4),("B",5)))
//    rdd1.cogroup(rdd2)
//    rdd1.join(rdd2)
//    rdd1.leftOuterJoin(rdd2)
//    rdd1.rightOuterJoin(rdd2)


  }
}
