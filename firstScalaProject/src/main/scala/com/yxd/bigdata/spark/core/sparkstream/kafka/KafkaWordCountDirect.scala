package com.yxd.bigdata.spark.core.sparkstream.kafka

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ibf on 02/26.
  */
object KafkaWordCountDirect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("kafka-wordcount-direct")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(30))

    val kafkaParams = Map(
      "metadata.broker.list" -> "hadoop-senior01:9092,hadoop-senior01:9093,hadoop-senior01:9094,hadoop-senior01:9095",
      "auto.offset.reset" -> "smallest" // 当程序调用的是createDirectStream(ssc, params, topics)这个API的时候，该参数有效
    )
    val topics = Set("beifeng")
    val dstream = KafkaUtils.createDirectStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](
      ssc, // 指定上下文
      kafkaParams, // 指定连接kafka的参数，内部使用的是kafka的SimpleConsumerAPI
      topics // topic名称
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
