package com.yxd.bigdata.spark.core.yxdlog

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by Administrator on 2017/1/17 0017.
 */
object yxdLogAnalyzer {


  def main(args: Array[String]) {
    //define  sparkconf
    val sparkCof =  new SparkConf()
    .setMaster("local[*]")
    .setAppName("yxdLogAnalyzer")

    //define sparkcontext
    val sparkContext =  SparkContext.getOrCreate(sparkCof)

    //filter data
    val path = ""
    val pathRdd = sparkContext.textFile(path)

    val filterRdd = pathRdd.filter(line => line.length>0)//过滤长度
    .filter(line => yxdLogData.isVail(line))//过滤符合格式
    .map(line => yxdLogData.parseLogLine(line))//映射成我们想要的数组格式

    /**
     * 需求一：计算整个响应时间的最大值和最小值，平均值
     */
    val durTimeRdd = filterRdd.map(line => line.durtime )
    val sum = durTimeRdd.sum()
    val max = durTimeRdd.max()
    val min = durTimeRdd.min()
    val size = durTimeRdd.count()
    val avg = 1.0*sum/size

    print(s" avg:${avg}, max:${max},min:${min}")


    /**
     * 计算返回码计数
     */

    val resCodeRdd = filterRdd.map(line => (line.responderCode,1))
      .reduceByKey(_+_)

    print(s"""resCode:${resCodeRdd.collect().mkString(",")}""")


    /**
     * 计算IP唯一数 并且大于2
     */
    val ipRdd = filterRdd.map(line => (line.ip,1))
    .reduceByKey(_+_).filter(tuple => tuple._2>2)

    print(s"""iprdd:${ipRdd.collect().mkString(",")}""")


    /**
     * 最后计算一个rescode的排列数前三的
     */
    val resCodeTopRdd = filterRdd.map(line => (line.responderCode,1))
      .reduceByKey(_+_).top(3)(yxdDefineSort.TupleSort)

    print(s"""resCode:${resCodeTopRdd.mkString(",")}""")

    //stop sparkcof
    sparkContext.stop()
  }

}
