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
    val path = "/logtestdata/access.log"
    val pathRdd = sparkContext.textFile(path)

    val filterRdd = pathRdd.filter(line => line.length>0)//过滤长度
    .filter(line => yxdLogData.isVail(line))//过滤符合格式
    .map(line => yxdLogData.parseLogLine(line))//映射成我们想要的数组格式

    //优化措施 多次使用的Rdd 缓存下
    filterRdd.cache()
    /**
     * 需求一：计算整个响应时间的最大值和最小值，平均值
     */
    val durTimeRdd = filterRdd.map(line => line.durtime )
    durTimeRdd.cache()
    val sum = durTimeRdd.sum()
    val max = durTimeRdd.max()
    val min = durTimeRdd.min()
    val size = durTimeRdd.count()
    val avg = 1.0*sum/size

    durTimeRdd.unpersist()
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
    val blackIP = Array("200-55-104-193.dsl.prima.net.ar", "10.0.0.153", "208-38-57-205.ip.cal.radiant.net")
    // 由于集合比较大，将集合的内容广播出去
    val broadCastIP = sparkContext.broadcast(blackIP)
    val ipRdd = filterRdd.filter(line => !broadCastIP.value.contains(line.ip))
      .map(line => (line.ip,1))
    .reduceByKey(_+_).filter(tuple => tuple._2>2)

    print(s"""iprdd:${ipRdd.collect().mkString(",")}""")


    /**
     * 最后计算一个rescode的排列数前三的
     */
    val resCodeTopRdd = filterRdd.map(line => (line.responderCode,1))
      .reduceByKey(_+_).top(3)(yxdDefineSort.TupleSort)

    print(s"""resCode:${resCodeTopRdd.mkString(",")}""")
    filterRdd.unpersist()
    //stop sparkcof
    sparkContext.stop()
  }

}
