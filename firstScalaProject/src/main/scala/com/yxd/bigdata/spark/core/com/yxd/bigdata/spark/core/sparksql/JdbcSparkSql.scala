package com.yxd.bigdata.spark.core.com.yxd.bigdata.spark.core.sparksql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import java.util.Properties

/**
 * Created by 20160905 on 2017/2/21.
 */
object JdbcSparkSql {
  def main(args: Array[String]) : Unit = {
    //基本结构
    val sparkConf  = new SparkConf()
      .setMaster("local[*]")
      .setAppName("JdbcSparkSql")

    val sparkContext = SparkContext.getOrCreate(sparkConf)
    //SqlContext 和 HiveContext 区别 后者继承了sqlContext ;不需要hive相关内容就
    //使用前者，需要就使用后者；后者缺陷就是易产生perm menmery 内存溢出
    val sqlContext = new HiveContext(sparkContext)


    // 第一步：同步hive的dept表到mysql中
    sqlContext
      .read
      .table("default.stats_hourly")
    .show()

    //定义链接属性
    val  (url,name,password) =("jdbc:mysql://hadoop1:3306/metastore", "root", "root")

    val pros = new Properties()
    pros.put("name", name)
    pros.put("password",password)

    //显示数据库及表
    sqlContext.sql("show databases").show

    //读hive表数据进入展示
    //sqlContext.read.table()



    //结束
  }

}
