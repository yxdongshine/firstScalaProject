package com.yxd.bigdata.spark.core.sparkstream.StreamKafka

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by 20160905 on 2017/3/6.
 */
object StreamSql {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    .setAppName("StreamSql")
    .setMaster("local[*]")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://hadoop1:9000/spark-history")

    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val sparkStream = new StreamingContext(sparkContext,Seconds(10))
    //val hiveContext = new HiveContext(sparkContext)

    //创建数据连接方式

    val topic =Map("yxdkafka0"->3)//消费yxdkafka0 消息主题 三个线程对应三个区
    val paraMap = Map(
        "group.id" -> "streaming-kafka",//消费组
        "zookeeper.connect" -> "hadoop1:2181/kafka",//连接zk消费
        "auto.offset.reset" -> "smallest"//从最小开始偏移量
      )

    //创建DStream
    val kafkaStream = KafkaUtils.createStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](
      sparkStream,
      paraMap,
      topic,
      StorageLevel.MEMORY_AND_DISK_SER_2 //存储级别
    )

    print("=====kafkaStream.print()=======")
    kafkaStream.print()

    //转换DStream
    val traRdd = kafkaStream
      .map(_._2) //返回第一个是消息的Key 第二个是值

    /*//类似ＲＤＤapi一样使用
    val wlRdd = traRdd
      .filter( line => line.nonEmpty)
      .flatMap(line => line.split(" ").map(world =>(world,1)))
      .reduceByKey(_ + _)*/

      /**
       *     这里stream转换成DataFlume
       *     dataFlume 和dataset 区别就是数据在转换时候才有数据 格式，而DataSet在底层已经有对象数据格式
       */


    val wlRdd = traRdd.transform(
        //这里是在dirver中运行的
        rdd => {


          //这里创建sqlContext
          val sqlContext = getSqlContext.getSqlContext(rdd.sparkContext)
          import  sqlContext.implicits._

          //这里在excutor上运行
          rdd
            .filter( line => line.nonEmpty)
            .flatMap(line => line.split(" ").map(world =>(world,1)))
          .toDF("world","num")
          .registerTempTable("tem_world_num")


          //sql
          sqlContext.sql("select world ,count(world) as num from tem_world_num twn group by twn.world")
          .map(
              row =>{
                val world = row.getAs[String]("world")
                val num = row.getAs[Long]("num")
                (world,num)
              }
            )


         }
      )

    //将Rdd数据写入hive中
    /**
     * http://www.cnblogs.com/awishfullyway/p/6485156.html
     */
    //hiveContext.
    print("=====wlRdd.print()=======")
    wlRdd.print() // 打印数据

    // 启动开始处理
    sparkStream.start()
    sparkStream.awaitTermination() // 等等结束，监控一个线程的中断操作

  }

  object getSqlContext{

    @transient private var instance:SQLContext =_

    def getSqlContext(sparkContext: SparkContext):SQLContext={
      synchronized {
      if(instance ==null){
            instance = new SQLContext(sparkContext)
         }
      }
      instance
    }

  }
}
