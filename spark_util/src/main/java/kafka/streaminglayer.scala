package kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{OffsetRange, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.zookeeper.data.ACL
import org.apache.zookeeper.data.Id

import scala.collection.mutable.ListBuffer
import scala.concurrent.JavaConversions
import org.apache.zookeeper.ZooDefs
import scala.collection.JavaConversions._


object streaminglayer {

  //
  def readOffsets(topics: Seq[String], groupId:String,zkUtils:ZkUtils):

  //val zkUtils: ZkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false)
  Map[TopicPartition, Long] = {

    val topicPartOffsetMap = collection.mutable.HashMap.empty[TopicPartition, Long]
    val partitionMap = zkUtils.getPartitionsForTopics(topics)

    // /consumers/<groupId>/offsets/<topic>/
    partitionMap.foreach(topicPartitions => {
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
      topicPartitions._2.foreach(partition => {
        val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + partition

        try {
          val offsetStatTuple = zkUtils.readData(offsetPath)
          if (offsetStatTuple != null) {
            //LOGGER.info("retrieving offset details - topic: {}, partition: {}, offset: {}, node path: {}", Seq[AnyRef](topicPartitions._1, partition.toString, offsetStatTuple._1, offsetPath): _*)

            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)),
              offsetStatTuple._1.toLong)
          }

        } catch {
          case e: Exception =>
            //LOGGER.warn("retrieving offset details - no previous node exists:" + " {}, topic: {}, partition: {}, node path: {}", Seq[AnyRef](e.getMessage, topicPartitions._1, partition.toString, offsetPath): _*)

            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), 0L)
        }
      })
    })

    topicPartOffsetMap.toMap
  }


  def persistOffsets(offsets: Seq[OffsetRange], groupId: String, storeEndOffset: Boolean,zkUtils:ZkUtils): Unit = {
    offsets.foreach(or => {
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic);

      val acls = new ListBuffer[ACL]()
      val acl = new ACL
      acl.setId(ZooDefs.Ids.ANYONE_ID_UNSAFE)
      acl.setPerms(ZooDefs.Perms.ALL)
      acls += acl

      val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition;
      val offsetVal = if (storeEndOffset) or.untilOffset else or.fromOffset
      //zkUtils.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir + "/"
      //  + or.partition, offsetVal + "", JavaConversions.bufferAsJavaList(acls))
      zkUtils.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir + "/"
        + or.partition, offsetVal + "", acls.toList)

      //LOGGER.debug("persisting offset details - topic: {}, partition: {}, offset: {}, node path: {}", Seq[AnyRef](or.topic, or.partition.toString, offsetVal.toString, offsetPath): _*)
    })
  }
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("example")
      .setMaster("local[4]") //Uncomment this line to test while developing on a workstation
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.16.70.211:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("MAC_INFO_TOPIC")
    val zkUrl = "10.16.70.211:9092"
    val sessionTimeout = 3000
    val connectionTimeout = 3000

    val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout)
    val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)


    //  val stream = KafkaUtils.createDirectStream[String, String](
    //    ssc,
    //    PreferConsistent,
    //    Subscribe[String, String](topics, kafkaParams)
    //  )

    val inputDStream = KafkaUtils.createDirectStream(ssc, PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics, kafkaParams, readOffsets(topics,kafkaParams.apply("stream_mac_info").toString,zkUtils)))

    //inputDStream.map(record => (record.key, record.value))

    inputDStream.foreachRDD((rdd,batchTime) => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offset => println(offset.topic, offset.partition, offset.fromOffset,offset.untilOffset))
      //val newRDD = rdd.map(message => processMessage(message))
      //newRDD.count()
      //saveOffsets(topic,consumerGroupID,offsetRanges,hbaseTableName,batchTime) //save the offsets to HBase
      persistOffsets(offsetRanges.toSeq,kafkaParams.apply("stream_mac_info").toString,true,zkUtils)
    })
  }


}

