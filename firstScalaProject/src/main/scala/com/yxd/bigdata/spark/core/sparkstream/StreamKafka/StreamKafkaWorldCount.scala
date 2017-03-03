package com.yxd.bigdata.spark.core.sparkstream.StreamKafka

/**
 * Created by 20160905 on 2017/3/3.
 */

package com.yxd.bigdata.spark.core.sparkstream.kafka

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamKafkaWorldCount {

  def main(args: Array[String]) {


    val conf = new SparkConf()
      .setAppName("StreamKafkaWorldCount")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10)) //指定批次运行间隔时间

    //创建数据连接方式

    val topic =Map("yxdkafka0"->3)//消费yxdkafka0 消息主题 三个线程对应三个区
    val paraMap = Map(
      "group.id" -> "streaming-kafka",//消费组
      "zookeeper.connect" -> "hadoop1:2181/kafka",//连接zk消费
      "auto.offset.reset" -> "smallest"//从最小开始偏移量
    )

    //创建DStream
    val kafkaStream = KafkaUtils.createStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](
       ssc,
       paraMap,
      topic,
      StorageLevel.MEMORY_AND_DISK_SER_2 //存储级别
    )

    print("=====kafkaStream.print()=======")
    kafkaStream.print()

    //转换DStream
   val traRdd = kafkaStream
    .map(_._2) //返回第一个是消息的Key 第二个是值

/*    val resultWordCount = traRdd
      .filter(line => line.nonEmpty)
      .flatMap(line => line.split(" ").map((_, 1)))
      .reduceByKey(_ + _)*/



    //类似ＲＤＤapi一样使用
    val wlRdd = traRdd
      .filter( line => line.nonEmpty)
    .flatMap(line => line.split(" ").map(world =>(world,1)))
    .reduceByKey(_ + _)

    print("=====wlRdd.print()=======")
    wlRdd.print() // 打印数据

    // 启动开始处理
    ssc.start()
    ssc.awaitTermination() // 等等结束，监控一个线程的中断操作
  }

}
