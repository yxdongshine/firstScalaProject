package com.yxd.bigdata.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

/**
 * Created by 20160905 on 2017/2/27.
 */
public class ConsumerByThreads implements  Runnable{

    //broker 列表
    String brokerList = "hadoop1:9092,hadoop1:9093,hadoop1:9094";
    String topicName ;
    Integer threadNum ;
    Producer producer ;
    static final String DISS_KEY="key_" ;//key前缀
    static final String DISS_VALUE="value_" ;//value前缀
    Random random = new Random();

    /**
     * 带参数的狗仔函数
     * @param topicName 消息主题名称
     * @param threadNum 发送消息线程数量
     */
    public  ConsumerByThreads(String topicName , Integer threadNum){

        this.topicName = topicName;
        this.threadNum =threadNum;
        buildProducer();
    }

    /**
     * 开始构建一个producer生产者
     */
    public void buildProducer(){

        //创建Properties
        Properties properties = new Properties();
        //添加属性
        properties.put("metadata.broker.list",brokerList);
        /**
         * 默认是：DefaultEncoder, 指发送的数据类型是byte类型
         * 如果发送数据是string类型，必须更改StringEncoder
         */
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("partitioner.class","com.yxd.bigdata.spark.kafka.ConsumerPartitioner".trim());
        //创建ProducerConfig
        ProducerConfig producerConfig = new ProducerConfig(properties);
        //创建 producer
        producer = new Producer(producerConfig);
    }


    /**
     * 构建消息
     * @param threadNum
     * @return
     */
    public KeyedMessage<String, String> ProducerKeyMsg(Long threadNum){

        KeyedMessage<String, String> keyedMessage = new KeyedMessage(topicName,DISS_KEY+threadNum,DISS_VALUE+threadNum);
        return  keyedMessage;
    }


    /**
     * 发送消息
     * @param keyedMessage
     */
    public  void sendMsg(KeyedMessage<String, String> keyedMessage){

        producer.send(keyedMessage);

    }
    /**
     * 实现多线程
     */
    public void run() {

        for( Integer i =0 ; i<threadNum ; i++){
           new Thread(new Runnable() {
               public synchronized void run() {
                       while (ConsumerThreadsBasic.isRunning.get()) {
                           //多线程之间发送消息
                           //创建消息
                           KeyedMessage keyedMessage = ProducerKeyMsg(Thread.currentThread().getId());
                           //发送消息
                           sendMsg(keyedMessage);
                           //这里需要让出cpu
                           try {
                               wait(100);
                           } catch (InterruptedException e) {
                               // nothings
                           }
                       }
               }
           }).start();
        }

    }


    /**
     * 关闭生产者
     */
    public  void closeProducer(){
        producer.close();
    }

}
