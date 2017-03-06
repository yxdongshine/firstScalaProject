package com.yxd.bigdata.spark.core.sparkstream.StreamKafka

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{ SparkConf, SparkContext}

/**
 * Created by 20160905 on 2017/3/6.
 */
object StreamSparkMapWithState {
  def main(args: Array[String]) {


    val conf = new SparkConf()
      .setAppName("StreamSparkMapWithState")
      .setMaster("local[*]")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://hadoop1:9000/spark-history")
      .set("spark.yarn.historyServer.address"," http://hadoop1:18080")

    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(10)) //指定批次运行间隔时间
    // 当调用updateStateByKey函数API的时候，必须给定checkpoint dir
    // 路径对应的文件夹不能存在
    ssc.checkpoint("hdfs://hadoop1:9000/yxd/spark/streaming/chkdir06")
    /**
     *
     * @param key    DStream的key数据类型
     * @param values DStream的value数据类型
     * @param state  是StreamingContext中之前该key的状态值
     * @return
     */
    def mappingFunction(key: String, values: Option[Int], state: State[Long]): (String, Long) = {
      // 获取之前状态的值
      val preStateValue = state.getOption().getOrElse(0L)
      // 计算出当前值
      val currentStateValue = preStateValue + values.getOrElse(0)

      // 更新状态值
      state.update(currentStateValue)

      // 返回结果
      (key, currentStateValue)
    }
    val spec = StateSpec.function[String, Int, Long, (String, Long)](mappingFunction _)
      .numPartitions(10)
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

    //类似ＲＤＤapi一样使用
    val wlRdd = traRdd
      .filter( line => line.nonEmpty)
      .flatMap(line => line.split(" ").map(world =>(world,1)))
      .reduceByKey(_ + _)
      .mapWithState(
        spec
      )

    print("=====wlRdd.print()=======")
    wlRdd.print() // 打印数据


    //这里转换成sqlcontext



    // 启动开始处理
    ssc.start()
   // ssc.awaitTermination() // 等等结束，监控一个线程的中断操作
  }
}
