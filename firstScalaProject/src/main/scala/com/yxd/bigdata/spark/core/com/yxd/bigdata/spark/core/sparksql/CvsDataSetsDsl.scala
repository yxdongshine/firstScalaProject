package com.yxd.bigdata.spark.core.com.yxd.bigdata.spark.core.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by 20160905 on 2017/2/23.
 */
object CvsDataSetsDsl {
  def main(args: Array[String]) {
    //基本结构
    val sparkConf  = new SparkConf().setMaster("local[*]").setAppName("JdbcSparkSql")
    val sc = SparkContext.getOrCreate(sparkConf)
    //SqlContext 和 HiveContext 区别 后者继承了sqlContext ;不需要hive相关内容就
    //使用前者，需要就使用后者；后者缺陷就是易产生perm menmery 内存溢出
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._
    //cvs path
    val cvsPath = "/sparksql/csvdata"
    val formatType = "com.databricks.spark.csv"

    //现在dataFrame 读入展示
     val df = sqlContext
    .read
    .format(formatType)
      .option("header","false")
    .load(cvsPath)
    .toDF("id", "longitude", "latitude", "time") //隐士转换 成对象

    df.show(1)

    //注册成临时表
    df.registerTempTable("tmp_taxi")

    /*println("=======DataSets==========")
    //DataSets模式读入展示
    //按照格式读进展示
    val ds = sqlContext
    .read
    .format(formatType)
    .option("header","false")
    .load(cvsPath)
    .as[Taxi]

    ds.show(1)*/

    //println("=========sql=============")
    //展示临时表数据
    /*val sql = "" +
      "select * ,subString(tt.time,0,2) as hour from tmp_taxi as tt "+
    "order by hour  "
    sqlContext
    .sql(sql)
    .show()*/

    /**
     * 实现需求
     * 找出每个时间段（每个小时）载客次数最多的五个人数据
     */
    /**
     * 普通sql 还是很有难度的 （要借助存储过程）
     */
    println("=========sql1=============")
    val  sql1 ="" +
      "select  subString(tt.time,0,2)  as hour,id, count(id) as num  from tmp_taxi as tt " +
      "group by subString(tt.time,0,2) , id " +
      "order by hour ,num desc "
   /* sqlContext
    .sql(sql1)
    .show()*/


    /**
     * 利用窗口函数解决
     */
    println("=========sql2============")
    val sql2 ="" +
      "select * from ( " +
        "select *, " +
        "ROW_NUMBER() over(partition by hour ) as rn " +
        "from " +
          "(" +
            "select  subString(tt.time,0,2)  as hour,id, count(id) as num  from tmp_taxi as tt " +
            "group by subString(tt.time,0,2) , id " +
            "order by hour ,num desc "+
          ") as tth " +
      ") as tt " +
      "where tt.rn <= 5 "
    /*sqlContext
    .sql(sql2)
    .show()*/

    println("=========DSL============")
    /**
     * DSL 简单操作
     *
     *  "select  subString(tt.time,0,2)  as hour,id, count(id) as num  from tmp_taxi as tt " +
      "group by subString(tt.time,0,2) , id " +
      "order by hour ,num desc "
     */
     df
       .select(  substring($"time", 0, 2).as("hour") ,$"id" )
    .groupBy( "hour", "id")
    .agg(
         count("id").as("num")
       )
    .orderBy($"hour",$"num".desc)
    .limit(100)
    .show(100)



  }

}
case class Taxi(id: Int, longitude: Float,latitude :Float,count:Int)
