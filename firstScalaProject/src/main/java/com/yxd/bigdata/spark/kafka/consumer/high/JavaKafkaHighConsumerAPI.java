package com.yxd.bigdata.spark.kafka.consumer.high;

/**
 * Created by 20160905 on 2017/3/30.
 */
import com.yxd.bigdata.spark.kafka.FileStream.FileIo;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
public class JavaKafkaHighConsumerAPI {
    private static kafka.javaapi.consumer.ConsumerConnector consumer;

    static  FileWriter fw = null ;
    static  FileIo fileIo = null ;

    public static void consume() {

        String togicName = "yxdkafka0";

        Properties props = new Properties();
        // zookeeper 配置
        props.put("zookeeper.connect", "hadoop1:2181/kafka");

        // group 代表一个消费组
        props.put("group.id", "jd-group");

        // zk连接超时
        //props.put("zookeeper.session.timeout.ms", "4000");
        //props.put("zookeeper.sync.time.ms", "200");
        //props.put("auto.commit.interval.ms", "1000");
       // props.put("auto.offset.reset", "smallest");
        // 序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(togicName, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,
                keyDecoder, valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(togicName).get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext()){
            System.out.println(it.next().message());
            fileIo.writeFile(it.next().message(), fw);

        }

    }

    public static void main(String[] args) {

        try {
            fw =  new FileWriter("highConsumer.txt");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            System.out.println(e.getMessage());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        fileIo = new FileIo();

        while(true){
            consume();
        }
    }
}
