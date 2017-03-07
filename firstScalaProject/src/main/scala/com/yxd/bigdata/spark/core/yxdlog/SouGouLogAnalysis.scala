package com.yxd.bigdata.spark.core.yxdlog

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by 20160905 on 2017/1/20.
 */
object SouGouLogAnalysis {

  def main(args: Array[String]) {
    //create spark conf
    val conf = new SparkConf()
      .setAppName("SouGouLogAnalysis")
      .setMaster("local[*]")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://hadoop1:9000/spark-history")

    //create sparkcontext
    val sparkcontext = SparkContext.getOrCreate(conf)

    //add path rdd
    val  path = "/sougoulog/SogouQ.sample"
    val dataPathRdd = sparkcontext.textFile(path)

    //filter data
    val filterRdd = dataPathRdd
      .filter(line => line.length>0)
      .map(line => line.split("\t"))
    //filterRdd.foreach(stringArr => println(stringArr.length))
    .map(
      tuple => {
        tuple(0) = SouGouLogData.toHour(tuple(0)).toString
        tuple
      }
      )
    //filterRdd.foreach(stringArr =>stringArr.foreach(println(_)))

    //cache
    filterRdd.cache()
    //analysis mind
    val userRdd = filterRdd.map(tuple =>
      (tuple(0),tuple(1))
    ).distinct()
      //userRdd.foreach(stringArr => (print(stringArr)))
    .groupByKey()
    .map(tuple => {
      val key = tuple._1
      val values = tuple._2.toList.distinct.size
      (key,values)
    }).sortBy(tuple => tuple._1)


    //print(userRdd.collect().foreach(print))

    filterRdd.unpersist()
    userRdd.saveAsTextFile("/sougoulog/"+System.currentTimeMillis()+"/")
    //stop sparkcontext
    sparkcontext.stop()
  }
}
