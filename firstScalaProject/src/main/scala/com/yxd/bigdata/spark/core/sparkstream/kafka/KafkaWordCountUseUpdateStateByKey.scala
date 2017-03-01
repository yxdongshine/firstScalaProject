package com.yxd.bigdata.spark.core.sparkstream.kafka

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * Created by ibf on 02/26.
  */
object KafkaWordCountUseUpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("kafka-wordcount")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    // 当调用updateStateByKey函数API的时候，必须给定checkpoint dir
    // 路径对应的文件夹不能存在
    ssc.checkpoint("hdfs://hadoop-senior01:8020/beifeng/spark/streaming/chkdir01")

    // 初始化updateStateByKey用到的状态值
    // 从保存状态值的地方(HBase)读取状态值， 这里采用模拟的方式
    val initialRDD: RDD[(String, Long)] = sc.parallelize(
      Array(
        ("hadoop", 100L),
        ("spark", 25L)
      )
    )


    val kafkaParams = Map(
      "group.id" -> "streaming-kafka-01",
      "zookeeper.connect" -> "hadoop-senior01:2181/kafka",
      "auto.offset.reset" -> "largest"
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
      .updateStateByKey(
        (values: Seq[Int], state: Option[Long]) => {
          // 从value中获取累加值
          val sum = values.sum

          // 获取以前的累加值
          val oldStateSum = state.getOrElse(0L)

          // 更新状态值并返回
          Some(oldStateSum + sum)
        },
        new HashPartitioner(ssc.sparkContext.defaultParallelism), // 分区器
        initialRDD // 初始化状态值
      )

    // TODO: 将resultWordCount保存到HBase即可

    resultWordCount.print() // 这个也是打印数据

    // 启动开始处理
    ssc.start()
    ssc.awaitTermination() // 等等结束，监控一个线程的中断操作
  }
}
