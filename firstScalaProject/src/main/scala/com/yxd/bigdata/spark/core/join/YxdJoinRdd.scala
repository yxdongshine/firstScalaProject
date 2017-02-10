package com.yxd.bigdata.spark.core.join

import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by 20160905 on 2017/2/10.
 */
object YxdJoinRdd {

  def main(args: Array[String]) {
     val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("YxdJoinRdd")

    val sc = SparkContext.getOrCreate(sparkConf)
    //具体代码
    val rdd1 = sc.parallelize(
    Array(
      (1, "张三1"),
      (1, "张三2"),
      (2, "李四"),
      (3, "王五"),
      (4, "Tom"),
      (5, "Gerry"),
      (6, "莉莉")
    ), 1
    )

    val rdd2 = sc.parallelize(Array(
      (1, "上海"),
      (2, "北京1"),
      (2, "北京2"),
      (3, "南京"),
      (4, "纽约"),
      (6, "深圳"),
      (7, "香港")
    ), 1)







    Thread.sleep(1000000)
    sc.stop()





  }

}
