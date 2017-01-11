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

    //val str ="192.168.25.128^A1483327400.959^A/hadoop.gif?en=e_e&ca=%E6%90%9C%E7%B4%A2%E4%BA%8B%E4%BB%B6%E6%B5%81&ac=%E8%AF%A6%E6%83%85%E9%A1%B5%E9%9D%A2%E6%B5%8F%E8%A7%88%E4%BA%8B%E4%BB%B6&ver=1&pl=website&sdk=js&u_ud=758EDFE1-7B54-419D-A020-3A0BB6FD81E2&u_mid=ibeifeng&u_sd=969FED4E-DFEC-4531-920F-6D22ED66BB3A&c_time=1483327399640&l=en-us&b_iev=Mozilla%2F5.0%20(X11%3B%20Linux%20x86_64)%20AppleWebKit%2F534.26%2B%20(KHTML%2C%20like%20Gecko)%20Version%2F5.0%20Safari%2F534.26%2B&b_rst=1280*768"
   // val arr1= str.split("\\^A")
    //define cf
    val conf = new  SparkConf()
      .setMaster("local[*]")
      .setAppName("yxdtop")
    val sparkcontext = new SparkContext(conf)
    val  path = "/eventLogs/2017/01/02/FlumeData.1483327088054.log"
    val  pathRdd = sparkcontext.textFile(path)
    val  lengthFilterRdd = pathRdd.filter(line => line.length>0)
    val  arrthreeFilterRdd =  lengthFilterRdd.filter(line =>line.split("\\^A").size==3 )
    val  mapThreeRdd:RDD[(String,String,String)] = arrthreeFilterRdd.map(
    line => {
      print(line)
      val arr = line.split("\\^A")
      var date:String=null
      val  dateTimeSArr = arr(1).split("\\.")
      var dateTime =""
      if(dateTimeSArr!=null && dateTimeSArr.length>=0){
        dateTime = dateTimeSArr(0)
      }else{
        dateTime = arr(1)
      }
      val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      date= sdf.format(new Date((dateTime.toLong)))
      (date,arr(0),arr(2))

    }
    ).filter( tuple =>tuple._1!=null || tuple._1.length>0)



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
