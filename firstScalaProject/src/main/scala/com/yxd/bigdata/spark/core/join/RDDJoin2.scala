package com.yxd.bigdata.spark.core.join

import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD数据Join相关API讲解
  * Created by ibf on 02/09.
  */
object RDDJoin2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD-Join")
    val sc = SparkContext.getOrCreate(conf)

    // ==================具体代码======================
    // 模拟数据产生， 添加map、reduceByKey、mapPartitions等api的主要功能是给rdd1和rdd2中添加一个分区器(表示当前rdd是存在shuffle过程的)
    val rdd1 = sc.parallelize(Array(
      (1, "张三1"),
      (1, "张三2"),
      (2, "李四"),
      (3, "王五"),
      (4, "Tom"),
      (5, "Gerry"),
      (6, "莉莉")
    ), 1).map(x => (x, null)).reduceByKey((x,y) => x, 1).mapPartitions(
      iter => iter.map(tuple => tuple._1),
      true // 使用上一个RDD的分区器，false表示不使用, 设置为None
    )

    val rdd2 = sc.parallelize(Array(
      (1, "上海"),
      (2, "北京1"),
      (2, "北京2"),
      (3, "南京"),
      (4, "纽约"),
      (6, "深圳"),
      (7, "香港")
    ), 1).map(x => (x, null)).reduceByKey((x,y) => x, 1).mapPartitions(
      iter => iter.map(tuple => tuple._1),
      true // 使用上一个RDD的分区器，false表示不使用, 设置为None
    )

    // 调用RDD API实现内连接
    val joinResultRDD = rdd1.join(rdd2).map {
      case (id, (name, address)) => {
        (id, name, address)
      }
    }
    println("----------------")
    joinResultRDD.foreachPartition(iter => {
      iter.foreach(println)
    })

    // 休眠为了看4040页面
        Thread.sleep(1000000)
  }
}
