package com.yxd.bigdata.spark.core.sparkstream.StreamKafka

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by 20160905 on 2017/3/3.
 */
object KafkaWordDirectCountDirect {

  def main(args: Array[String]) {


    val conf = new SparkConf()
      .setAppName("KafkaWordDirectCountDirect")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10)) //指定批次运行间隔时间

    //创建数据连接方式

    val topic =Set("yxdkafka0")//消费yxdkafka0
    val paraMap = Map(
        "metadata.broker.list" -> "hadoop1:9092,hadoop1:9093,hadoop1:9094",
        "auto.offset.reset" -> "smallest" // 当程序调用的是createDirectStream(ssc, params, topics)这个API的时候，该参数有效
      )

    //创建DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](
      ssc,
      paraMap,
      topic
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
