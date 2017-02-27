package com.yxd.bigdata.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by 20160905 on 2017/2/27.
 */
public class ConsumerTest0 {

    public static void main(String[] args) {

        //broker 列表
        String brokerList = "hadoop1:9092,hadoop1:9093,hadoop1:9094";

        //创建Properties
        Properties properties = new Properties();
        //添加属性
        properties.put("metadata.broker.list",brokerList);
        /**
         * 默认是：DefaultEncoder, 指发送的数据类型是byte类型
         * 如果发送数据是string类型，必须更改StringEncoder
         */
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        //创建ProducerConfig
        ProducerConfig producerConfig = new ProducerConfig(properties);
        //创建 producer
        Producer producer  = new Producer(producerConfig);

        //创建消息
        KeyedMessage keyedMessage = new KeyedMessage("yxdkafka0","key","value");

        //发送消息
        producer.send(keyedMessage);


        //调用关闭
        producer.close();
    }
}
