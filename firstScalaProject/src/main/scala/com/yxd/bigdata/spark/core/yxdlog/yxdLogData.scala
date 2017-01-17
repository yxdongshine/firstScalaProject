package com.yxd.bigdata.spark.core.yxdlog

import scala.util.matching.Regex


case class  yxdLogData(
                        ip:String ,//ip
                        clientid:String ,//客户端编号
                        userId:String ,//用户编号
                        date:String, //日期
                        method:String,//请求方式
                        endpoint: String, // 请求的资源
                        protocol: String, // 请求的协议名称
                        responderCode:Int ,//响应码
                        durtime:Long //整个时间

                        )
/**
 * Created by Administrator on 2017/1/17 0017.
 *
 * 日志数据对象
 */
object yxdLogData {

  val PARTTERN: Regex =
    """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  def  isVail(line: String ): Boolean ={
    var isVail = false
    val  option:Option[String] = PARTTERN.findFirstIn(line)
    if(!option.isEmpty){
      isVail = true
    }
    isVail

  }


  def parseLogLine(line: String): yxdLogData = {

    // 从line中获取匹配的数据
    val options = PARTTERN.findFirstMatchIn(line)

    // 获取matcher
    val matcher = options.get

    // 构建返回值
    yxdLogData(
      matcher.group(1), // 获取匹配字符串中第一个小括号中的值
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8).toInt,
      matcher.group(9).toLong
    )



  }

}

