package com.yxd.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 本地测试TopN<br/>
  * 计算一个文本文件中的单词出现，最终输出前3个
  * Created by ibf on 01/08.
  */
object TOPNSparkCore {
  // NOTO: 所有的main方法必须放到object中才能够执行
  // http://spark.apache.org/docs/1.6.0/programming-guide.html#initializing-spark
  // 由于需要读取HDFS上的文件数据，将hdfs的配置文件添加到项目中，放到resources文件夹中
  def main(args: Array[String]): Unit = {
    // Step1: Creating SparkContext
    val conf = new SparkConf()
      // 打包的时候setMaster必须注释掉
      //      .setMaster("local[*]") // 本地运行
      .setAppName("topN") // 应用名称
    val sc = new SparkContext(conf)


    // 保存在HDFS上的文本文件或者文件夹
    var path = "/beifeng/wordcount"
    // window本地文件
    //    path = "file:///D:/abc.txt"

    // 创建RDD
    val rdd = sc.textFile(path)

    // RDD操作，获取TopN
    val N = 3
    val wordCountRDD = rdd
      // 数据过滤
      .filter(line => line.length > 0)
      // 数据转换
      .flatMap(line => line.split(" ").map(word => (word, 1)))
      // 计算每个word单词的出现数量
      .reduceByKey((a, b) => a + b)

    // 获取最大的
    val topNBig: Array[(String, Int)] = wordCountRDD.top(N)(ord = new Ordering[(String, Int)]() {
      /**
        * 按照给定的排序规则进行排序操作
        *
        * @param x
        * @param y
        * @return
        */
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        // 先按照出现次数排序，然后再按照word进行排序
        // TOPN获取值的时候，次数排序降序排列，word升序排列
        val tmp = x._2.compare(y._2)
        if (tmp == 0) y._1.compare(x._1)
        else tmp
      }
    })

    // 获取最小的
    val topNSmall = wordCountRDD.top(3)(ord = new Ordering[(String, Int)]() {
      /**
        * 按照给定的排序规则进行排序操作
        *
        * @param x
        * @param y
        * @return
        */
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        // 先按照出现次数排序，然后再按照word进行排序
        // TOPN获取值的时候，次数排序降序排列，word升序排列
        val tmp = x._2.compare(y._2)
        if (tmp == 0) -(y._1.compare(x._1))
        else -tmp
      }
    })

    // 结果输出
    topNBig.foreach(println)
    topNSmall.foreach(println)

    // 在执行完成后，进行一个资源的关闭，也可以不进行显示的stop操作，因为SparkContext在内部添加一个JVM钩子，在JVM退出的时候进行关闭操作
    sc.stop()
  }
}
