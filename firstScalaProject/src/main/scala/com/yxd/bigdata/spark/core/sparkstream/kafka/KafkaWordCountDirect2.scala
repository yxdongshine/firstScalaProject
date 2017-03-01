package com.yxd.bigdata.spark.core.sparkstream.kafka

import scala.collection.mutable
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulable, AccumulableParam, SparkConf, SparkContext}

/**
  * Created by ibf on 02/26.
  */
object KafkaWordCountDirect2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("kafka-wordcount-direct2")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(30))
    val accumu = DroppedAccumulable.getInstance(sc)

    val kafkaParams = Map(
      "metadata.broker.list" -> "hadoop-senior01:9092,hadoop-senior01:9093,hadoop-senior01:9094,hadoop-senior01:9095"
    )

    // TODO: 从某一个存储offset的地方读取offset偏移量数据, redis\hbase\其他地方.....
    val fromOffsets = Map(
      TopicAndPartition("beifeng", 0) -> -1L, // 如果这里给定的偏移量是异常的，会直接从kafka中读取偏移量数据(largest)
      TopicAndPartition("beifeng", 1) -> 0L,
      TopicAndPartition("beifeng", 2) -> 0L,
      TopicAndPartition("beifeng", 3) -> 0L
    )


    val dstream = KafkaUtils.createDirectStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder, String](
      ssc, // 上下文
      kafkaParams, // kafka连接
      fromOffsets,
      (message: MessageAndMetadata[String, String]) => {
        // 这一块儿在Executor上被执行
        // 更新偏移量offset
        val topic = message.topic
        val paritionID = message.partition
        val offset = message.offset
        accumu += (topic, paritionID) -> offset
        // 返回value的数据
        message.message()
      }
    )

    val resultWordCount = dstream
      .filter(line => line.nonEmpty)
      .flatMap(line => line.split(" ").map((_, 1)))
      .reduceByKey(_ + _)


    resultWordCount.foreachRDD(rdd => {
      // 在driver上执行
      try {
        rdd.foreachPartition(iter => {
          // 代码在executor上执行
          // TODO: 这里进行具体的数据保存操作
          iter.foreach(println)
        })

        // TODO: 在这里更新offset, 将数据写入到redis\hbase\其他地方.....
        accumu.value.foreach(println)
      } catch {
        case e: Exception => // nothings
      }
    })


    // 启动开始处理
    ssc.start()
    ssc.awaitTermination() // 等等结束，监控一个线程的中断操作
  }
}


object DroppedAccumulable {
  private var instance: Accumulable[mutable.Map[(String, Int), Long], ((String, Int), Long)] = null

  def getInstance(sc: SparkContext): Accumulable[mutable.Map[(String, Int), Long], ((String, Int), Long)] = {
    if (instance == null) {
      synchronized {
        if (instance == null) instance = sc.accumulable(mutable.Map[(String, Int), Long]())(param = new AccumulableParam[mutable.Map[(String, Int), Long], ((String, Int), Long)]() {
          /**
            * 将t添加到r中
            *
            * @param r
            * @param t
            * @return
            */
          override def addAccumulator(r: mutable.Map[(String, Int), Long], t: ((String, Int), Long)): mutable.Map[(String, Int), Long] = {
            val oldOffset = r.getOrElse(t._1, t._2)
            if (t._2 >= oldOffset) r += t
            else r
          }

          override def addInPlace(r1: mutable.Map[(String, Int), Long], r2: mutable.Map[(String, Int), Long]): mutable.Map[(String, Int), Long] = {
            r2.foldLeft(r1)((r, t) => {
              val oldOffset = r.getOrElse(t._1, t._2)
              if (t._2 >= oldOffset) r += t
              else r
            })
          }

          override def zero(initialValue: mutable.Map[(String, Int), Long]): mutable.Map[(String, Int), Long] = mutable.Map.empty[(String, Int), Long]
        })
      }
    }

    // 返回结果
    instance
  }
}