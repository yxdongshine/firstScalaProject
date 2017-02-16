package com.yxd.bigdata.spark.core.accumulator

import org.apache.spark.{AccumulableParam, SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

/**
  * Spark累加器
  * Created by ibf on 02/15.
  */
object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //.setMaster("local[*]") // local模式下默认不进行失败重启机制
      .setMaster("local[*,4]") // 开启local模式的失败重启机制，重启次数4-1=3次
      .setAppName("accumulator")
    val sc = SparkContext.getOrCreate(conf)

    // ===============================
    val rdd = sc.parallelize(Array(
      "hadoop,spark,hbase",
      "spark,hbase,hadoop",
      "",
      "spark,hive,hue",
      "spark,hadoop",
      "spark,,hadoop,hive",
      "spark,hbase,hive",
      "hadoop,hbase,hive",
      "hive,hbase,spark,hadoop",
      "hive,hbase,hadoop,hue"
    ), 5)

    // 需求一：实现WordCount程序，同时统计输入的记录数量以及最终输出结果的数量
    val inputRecords = sc.accumulator(0, "Input Record Size")
    val outputRecords = sc.accumulator(0, "Output Record Size")
    rdd.flatMap(line => {
      // 累计数量
      inputRecords += 1
      val nline = if (line == null) "" else line
      // 进行数据分割、过滤、数据转换
      nline.split(",")
        .map(word => (word.trim, 1)) // 数据转换
        .filter(_._1.nonEmpty) // word非空，进行数据过滤
    })
      .reduceByKey(_ + _)
      .foreachPartition(iter => {
        iter.foreach(record => {
          // 累计数据
          outputRecords += 1
          println(record)
        })
      })

    println(s"Input Size:${inputRecords.value}")
    println(s"Ouput Size:${outputRecords.value}")

    // 需求二：假设wordcount的最终结果可以在driver/executor节点的内存中保存下，要求不通过reduceByKey相关API实现wordcount程序
    /**
      * 1. 每个分区进行wordcount的统计，将结果保存到累加器中
      * 2. 当分区全部执行完后，各个分区的累加器数据进行聚合操作
      */
    val mapAccumulable = sc.accumulable(mutable.Map[String, Int]())(MapAccumulableParam)
    try
      rdd.foreachPartition(iter => {
        val index = Random.nextInt(2) // index的取值范围[0,1]
        iter.foreach(line => {
          val r = 1 / index
          print(r)
          val nline = if (line == null) "" else line
          // 进行数据分割、过滤、数据转换
          nline.split(",")
            .filter(_.trim.nonEmpty) // 过滤空单词
            .map(word => {
            mapAccumulable += word // 统计word出现的次数
          })
        })
      })
    catch {
      case e: Exception => println(s"异常:${e.getMessage}")
    }
    println("result================")
    mapAccumulable.value.foreach(println)

    Thread.sleep(100000)
  }
}


object MapAccumulableParam extends AccumulableParam[mutable.Map[String, Int], String] {
  /**
    * 添加一个string的元素到累加器中
    *
    * @param r
    * @param t
    * @return
    */
  override def addAccumulator(r: mutable.Map[String, Int], t: String): mutable.Map[String, Int] = {
    r += t -> (1 + r.getOrElse(t, 0))
  }

  /**
    * 合并两个数据
    *
    * @param r1
    * @param r2
    * @return
    */
  override def addInPlace(r1: mutable.Map[String, Int], r2: mutable.Map[String, Int]): mutable.Map[String, Int] = {
    r2.foldLeft(r1)((a, b) => {
      a += b._1 -> (a.getOrElse(b._1, 0) + b._2)
    })
  }

  /**
    * 返回初始值
    *
    * @param initialValue
    * @return
    */
  override def zero(initialValue: mutable.Map[String, Int]): mutable.Map[String, Int] = initialValue
}

