package com.yxd.bigdata.spark.kafka.consumer.lower;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ibf on 12/21.
 */
public class JavaKafkaSimpleConsumerAPITest {
    public static void main(String[] args) {
        JavaKafkaSimpleConsumerAPI example = new JavaKafkaSimpleConsumerAPI(0);
        long maxReads = 20;
        // 获取topic名称为beifeng的，分区id为0的分区中的数据
        String topic = "yxdkafka0";
        int partitionID = 0;

        KafkaTopicPartitionInfo topicPartitionInfo0 = new KafkaTopicPartitionInfo(topic, 0);

        List<KafkaBrokerInfo> seeds = new ArrayList<KafkaBrokerInfo>();
        seeds.add(new KafkaBrokerInfo("hadoop1", 9092));
        seeds.add(new KafkaBrokerInfo("hadoop1", 9093));
        seeds.add(new KafkaBrokerInfo("hadoop1", 9094));

        //一直读取
        for(int i=0 ;i < Long.MAX_VALUE;i++){
            try {
                example.run(maxReads, topicPartitionInfo0, seeds);

            } catch (Exception e) {
                e.printStackTrace();
            }

            // 获取该topic所属的所有分区ID列表
            System.out.println(example.fetchTopicPartitionIDs(seeds, topic, 100000, 64 * 1024, "client-id"));

        }
   }
}
