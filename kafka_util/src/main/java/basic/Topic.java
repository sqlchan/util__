package basic;

import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Map;
import scala.collection.Seq;

import java.util.Properties;

public class Topic {
    private static final String zkUrl="192.168.238.133:2181";
    private static final Logger logger= LoggerFactory.getLogger(Topic.class);
    //private static ZkUtils zkUtils=getZkUtils();

    public static void main(String[] args) {
        //createTopic("hafaf", 3, 1,  new Properties());  //新建主题
        //modifyTopicConfig( "hafaf");   //修改主题配置
        //changePartitionAndReplication("hafaf");  //修改分区和副本

    }

    public static void createTopic(String topic, int partition, int replica, Properties properties){
        ZkUtils zkUtils=getZkUtils();
        try {
            if(!AdminUtils.topicExists(zkUtils,topic)){
                AdminUtils.createTopic(zkUtils,topic,partition,replica,properties,
                        AdminUtils.createTopic$default$6());
                logger.warn("create new topic");
            }
        }catch (Exception e){
            logger.error(e.getMessage());
        }finally {
            zkUtils.close();
        }
    }

    public static void modifyTopicConfig(String topic){
        Properties properties=new Properties();
        properties.setProperty("max.message.bytes","111111");

        ZkUtils zkUtils=getZkUtils();
        try {
            Properties curProp=AdminUtils.fetchEntityConfig(zkUtils,ConfigType.Topic(),topic);
            curProp.putAll(properties);
            AdminUtils.changeTopicConfig(zkUtils,topic,curProp);
            logger.warn("modifyTopicConfig");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            zkUtils.close();
        }
    }

    public static void changePartitionAndReplication(String topic){
        ZkUtils zkUtils=getZkUtils();
        try {
            Seq<BrokerMetadata> brokerMeta =AdminUtils.getBrokerMetadatas(zkUtils,
                    AdminUtils.getBrokerMetadatas$default$2(), AdminUtils.getBrokerMetadatas$default$3());
            Map<Object ,Seq<Object>> replicaAssign=AdminUtils.assignReplicasToBrokers(brokerMeta,4,1,
                    AdminUtils.assignReplicasToBrokers$default$4(),AdminUtils.assignReplicasToBrokers$default$5());
            AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils,topic,replicaAssign,null,true);
            logger.warn("changePartitionAndReplication");
        }catch (Exception e){
        }finally {
            zkUtils.close();
        }
    }

    public static void deleteTopic(String topic){
        ZkUtils zkUtils=getZkUtils();
        try {
            AdminUtils.deleteTopic(zkUtils,topic);
            logger.warn("deleteTopic");
        }catch (Exception e){
        }finally {
            zkUtils.close();
        }
    }

    public static ZkUtils getZkUtils(){
        return ZkUtils.apply(zkUrl,30000,30000,JaasUtils.isZkSecurityEnabled());
    }
}
