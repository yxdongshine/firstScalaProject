package com.yxd.bigdata.spark.core.sparkstream.kafka

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ibf on 02/26.
  */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("kafka-wordcount")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(30))

    val kafkaParams = Map(
      "group.id" -> "streaming-kafka-01",
      "zookeeper.connect" -> "hadoop-senior01:2181/kafka",
      "auto.offset.reset" -> "smallest"
    )
    val topics = Map("beifeng" -> 4) // topics中value是读取数据的线程数量，所以必须大于等于1
    val dstream = KafkaUtils.createStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](
      ssc, // 给定SparkStreaming上下文
      kafkaParams, // 给定连接kafka的参数信息 ===> 通过Kafka HighLevelConsumerAPI连接
      topics, // 给定读取对应topic的名称以及读取数据的线程数量
      StorageLevel.MEMORY_AND_DISK_2 // 指定数据接收器接收到kafka的数据后保存的存储级别
    ).map(_._2)

    val resultWordCount = dstream
      .filter(line => line.nonEmpty)
      .flatMap(line => line.split(" ").map((_, 1)))
      .reduceByKey(_ + _)

    resultWordCount.print() // 这个也是打印数据

    // 启动开始处理
    ssc.start()
    ssc.awaitTermination() // 等等结束，监控一个线程的中断操作
  }
}
