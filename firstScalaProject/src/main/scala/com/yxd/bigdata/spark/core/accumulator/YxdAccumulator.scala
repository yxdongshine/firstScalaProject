package com.yxd.bigdata.spark.core.accumulator

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by 20160905 on 2017/2/16.
 */
object YxdAccumulator {

  def main(args: Array[String]) {
      val sparkConf =  new SparkConf()
    .setMaster("local[*]")
    .setAppName("YxdAccumulator")

    val sparkContext = SparkContext.getOrCreate(sparkConf)

    //构建数据
    val rddData = sparkContext.parallelize(Array(
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
    ))

    // 定义累加器
    var inputAccumulator = sparkContext.accumulator(0,"input accumulator")
    var outAccumulator   = sparkContext.accumulator(0,"output accumulator")
    //计算worldcount
     rddData.flatMap(
    line => {
      inputAccumulator += 1
      line.filter(_ != null)
      .split(",")
      .map(mline => (mline,1) )
    }
    )
    .reduceByKey(_+_)
    .foreachPartition(
     part => {
       part.foreach{
        line =>{
          outAccumulator += 1
          println(line)
        }

       }
     })


    println({inputAccumulator.value})
    println({outAccumulator.value})


    println(s"=================")
    //实现复杂累加器分区统计
    //广播自定义复杂累加器
    val defineSelfAccumulableParam = sparkContext.accumulable(mutable.Map[String, Int]())(DefineSelfAccumulableParam)
    rddData
      .foreachPartition(
       part => {
         part.foreach(
           line => {
             line.filter(_ != null)
               .split(",")
               .map(
                 mline => defineSelfAccumulableParam += mline
               )
           }
         )
       }
      )


    //自动以输出
    defineSelfAccumulableParam.value.foreach(println)


/*
    val fmc = fmRddData.collect()

    fmc
*/
    sparkContext.stop


  }

}
