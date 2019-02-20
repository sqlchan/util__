package basic;


import org.junit.jupiter.api.Test;

import java.util.Set;


public class KafkaUtilTest {

    @Test
    public void testGetAllGroupsForTopic() {

//        // 170 集群
//        // topic : topic_advert_impress
//        Set<String> consumerSet = KafkaUtil.getAllGroupsForTopic("10.170.0.6:9092,10.170.0.7:9092,10.170.0.8:9092", "topic_advert_impress");

        Set<String> consumerSet = KafkaUtil.getAllGroupsForTopic("10.66.175.26:9092", "MAC_INFO_TOPIC");

        for (String tmp : consumerSet) {
            System.out.println(tmp);
        }

    }

}
