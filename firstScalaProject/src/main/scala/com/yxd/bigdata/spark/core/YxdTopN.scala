package com.yxd.bigdata.spark.core

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by 20160905 on 2017/1/11.
 */
object YxdTopN {

  def main(args: Array[String]) {
      val conf = new  SparkConf()
        .setMaster("local[*]")
        .setAppName("yxdtop")
      val sparkConf = new SparkContext(conf)

    val  path = "/eventLogs/2016/12/21/FlumeData.1482391467667.log"
    val rdd =sparkConf.textFile(path)
      .filter(line => line.length > 0)
      .flatMap(line => line.split(" ").map((_, 1)))
    .reduceByKey(_+_)
    .map(tuple => (tuple._2,tuple._1))
    .sortByKey(ascending = false)
    .map(tuple => (tuple._2,tuple._1))

    rdd.foreach(print)
    sparkConf.stop()
  }

}
