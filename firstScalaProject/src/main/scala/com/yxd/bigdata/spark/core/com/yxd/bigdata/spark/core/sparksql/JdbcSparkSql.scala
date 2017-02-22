package com.yxd.bigdata.spark.core.com.yxd.bigdata.spark.core.sparksql

import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import java.util.Properties

/**
 * Created by 20160905 on 2017/2/21.
 */
object JdbcSparkSql {
  def main(args: Array[String]) : Unit = {
    //基本结构
    val sparkConf  = new SparkConf().setMaster("local[*]").setAppName("JdbcSparkSql")

    val sc = SparkContext.getOrCreate(sparkConf)
    //SqlContext 和 HiveContext 区别 后者继承了sqlContext ;不需要hive相关内容就
    //使用前者，需要就使用后者；后者缺陷就是易产生perm menmery 内存溢出
    val sqlContext = new HiveContext(sc)

    //定义链接属性
    val  (url,user,password) =("jdbc:mysql://hadoop1:3306/metastore", "root", "root")

    val pros = new Properties()
    pros.put("user", user)
    pros.put("password",password)

    //显示数据库及表
    sqlContext
      .sql("show tables")
      .show()

    //读hive表数据进入展示
    sqlContext
      .read
      .table("stats_hourly")
      .show()

    // 第一步：同步hive的stats_hourly表到mysql中
/*    sqlContext
      .read
      .table("stats_hourly")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url,"stats_hourly",pros)*/


    println("=========================================")
    //读RDMB数据进来展示
    sqlContext
      .read
    .jdbc(url,"stats_hourly",pros)
    .registerTempTable("tem_stats_hourly")

    println("================jion=========================")
    //开始join
    val jionStr =
    "select sh.*,tsh.platform_dimension_id as id from stats_hourly as sh " +
      "left join tem_stats_hourly as tsh on sh.platform_dimension_id = tsh.platform_dimension_id"
    sqlContext
    .sql(jionStr)
    .write
    .mode(SaveMode.Overwrite)
    //.format("parquet")
    //.save("/sparksql/parquet")
    .parquet("/sparksql/parquet")

    //写出到hdfs上 json格式

    //读展示
    sqlContext
    .read
    //.format("parquet")
    //.load("/sparksql/parquet")
    .parquet("/sparksql/parquet")//直接写指定格式加上路径 应该一样的
    .show()
    //结束
  }

}
