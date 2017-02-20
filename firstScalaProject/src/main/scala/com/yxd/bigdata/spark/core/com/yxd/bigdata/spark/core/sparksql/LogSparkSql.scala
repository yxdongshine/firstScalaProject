package com.yxd.bigdata.spark.core.com.yxd.bigdata.spark.core.sparksql

import com.yxd.bigdata.spark.core.yxdlog.yxdLogData
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by 20160905 on 2017/2/20.
 */
object LogSparkSql {

  def main(args: Array[String]) {
    //基本结构
    val  sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("LogSparkSql")

    val sparkContext = SparkContext.getOrCreate(sparkConf)

    //创建一个sqlContext 因为sparkContext是sqlConText一个入口
    val sqlContext = new SQLContext(sparkContext)

    //先加入隐士转换
    import  sqlContext.implicits._
    //加载内容形成RDD
    val path = "/logtestdata/access.log"
    val pathRdd = sparkContext.textFile(path)

    //新的内容知识 转成dataframe
    val dataFrame: DataFrame = pathRdd
      .filter(line => (line.trim().length >0 ) )
      .filter(line =>(yxdLogData.isVail(line.toString)))
      .map(
       line => {yxdLogData.parseLogLine(line.toString)}
        ).toDF()



    //注册临时表 因为多次用到 考虑效率问题
    dataFrame.registerTempTable("logDataFrame")

    //查看临时表logDataFrame数据结构：
    val selectSql  = "select * from logDataFrame"
    sqlContext.sql(selectSql).show(10)

    //使用sqarksql统计durtime的平均值，最大值，最小值
    val durtimeSql  =
      "select avg(durtime) as avgdurtime ,max(durtime) as maxdurtime,min(durtime) as mindurtime" +
      " from logDataFrame as ldf"

    sqlContext.sql(durtimeSql).show()


    //统计每个返回码的数量
    val responderCodeSql  =
    "select responderCode ,count(responderCode) as responderCodeNum " +
      "from logDataFrame as ldf " +
      "group by ldf.responderCode "

    sqlContext.sql(responderCodeSql).show()


    //统计IP超过N次
    val  N = 5
    val  ipSql =
      "select ip ,count(ip) as ipNum " +
        "from logDataFrame as ldf " +
        "group by ldf.ip " +
        "having ipNum >  " +N

    sqlContext.sql(ipSql).show()

    // 需求四：获取访问次数最多的前K个endpoint的值 ==> TopN

    val  endpointSql =
      "select endpoint ,count(endpoint) as endpointNum " +
        "from logDataFrame as ldf " +
        "group by ldf.endpoint " +
        "order by endpointNum DESC " +
        "limit "+N

    sqlContext.sql(endpointSql).show()
    //关闭资源

  }
}
