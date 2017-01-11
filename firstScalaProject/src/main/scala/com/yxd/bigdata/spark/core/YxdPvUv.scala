package com.yxd.bigdata.spark.core

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by 20160905 on 2017/1/11.
 */
object YxdPvUv {

  def main(args: Array[String]) {
    //define cf
    val scof = new SparkConf()

    //define sparkcontext
    val  sparkcontext =  SparkContext.getOrCreate(scof)
    val  path = "/eventLogs/2016/12/21/FlumeData.1482391467667.log"
    val  pathRdd = sparkcontext.textFile(path)
    val  lengthFilterRdd = pathRdd.filter(line => line.length>0)
    val  mapThreeRdd:RDD[(String,String,String)] = lengthFilterRdd.map(
    line => {
      val arr = line.split("^A")
      var date:String=null
      if(arr(1).length>0){
        val dateTime = arr(1).split(".")(0)
        val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        date= sdf.format(new Date((dateTime.toLong)))
      }
      (date,arr(0),arr(2))
    }
    ).filter(tuple =>tuple._1!=null || tuple._1.length>0)



    val mapTwoRdd:RDD[(String,String)] = mapThreeRdd.map(
    tuple =>{
      (tuple._1,tuple._3)
    }
    )

    val groupByKeyRdd:RDD[scala.Tuple2[String, scala.Iterable[String]]] = mapTwoRdd.groupByKey()
    val pvRdd:RDD[(String,Int)] = groupByKeyRdd.map(
    tuple => {
      val  date = tuple._1
      val  count = tuple._2.size
      (date,count)
    }
    )

    //print
    pvRdd.foreach(print(_))

    //output hdfs
    pvRdd.saveAsTextFile("/spark/pv")



    //start uv
    val uvMapTwoRdd:RDD[(String,Int)] = mapThreeRdd.map(
      tuple =>{
        (tuple._1,tuple._2)
      }
    ).distinct.map(tuple =>(tuple._1,1) )

    val reduceRdd:RDD[(String,Int)] = uvMapTwoRdd.reduceByKey(_+_)

    //print
    reduceRdd.foreach(print(_))

    //output hdfs
    reduceRdd.saveAsTextFile("/spark/uv")

  }

}
