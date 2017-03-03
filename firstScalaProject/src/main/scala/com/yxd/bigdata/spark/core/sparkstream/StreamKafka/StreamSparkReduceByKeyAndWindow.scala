package com.yxd.bigdata.spark.core.sparkstream.StreamKafka

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
 * Created by 20160905 on 2017/3/3.
 */
object StreamSparkReduceByKeyAndWindow {
  def main(args: Array[String]) {


    val conf = new SparkConf()
      .setAppName("StreamSparkUpdateStateBYKey")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(10)) //指定批次运行间隔时间

    // 当调用updateStateByKey函数API的时候，必须给定checkpoint dir
    // 路径对应的文件夹不能存在
    ssc.checkpoint("hdfs://hadoop1:9000/yxd/spark/streaming/chkdir01")
    // 初始化updateStateByKey用到的状态值
    // 从保存状态值的地方(HBase)读取状态值， 这里采用模拟的方式
    val initialRDD: RDD[(String, Long)] = sc.parallelize(
      Array(
        ("Master", 10L),
        ("Worker", 20L)
      )
    )

    //创建数据连接方式

    val topic =Map("yxdkafka0"->3)//消费yxdkafka0 消息主题 三个线程对应三个区
    val paraMap = Map(
        "group.id" -> "streaming-kafka",//消费组
        "zookeeper.connect" -> "hadoop1:2181/kafka",//连接zk消费
        "auto.offset.reset" -> "largest"//从最大开始偏移量
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
      .reduceByKeyAndWindow(
        (a:Int , b:Int) =>(a + b), //累加
        Seconds(30), //一次性统计窗口个数，必须 为批次间隔时间整数倍
        Seconds(10)//向前推进个数 必须 为批次间隔时间整数倍
      )

    print("=====wlRdd.print()=======")
    wlRdd.print() // 打印数据

    // 启动开始处理
    ssc.start()
    ssc.awaitTermination() // 等等结束，监控一个线程的中断操作
  }

}
