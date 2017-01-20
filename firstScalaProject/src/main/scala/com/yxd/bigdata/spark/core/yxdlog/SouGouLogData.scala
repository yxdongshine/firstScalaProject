package com.yxd.bigdata.spark.core.yxdlog

/**
 * Created by 20160905 on 2017/1/20.
 */
class SouGouLogData {



}

object  SouGouLogData{

  def  toHour(time : String):Int = {
       var hour = 0
       if(time.trim.length>0){
         hour =time.split(":")(1).toInt
       }

       hour
  }
}