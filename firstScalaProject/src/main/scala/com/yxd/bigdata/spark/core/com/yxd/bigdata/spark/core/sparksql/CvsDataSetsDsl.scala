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
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //cvs path
    val cvsPath = "/sparksql/csvdata"
    val formatType = "com.databricks.spark.csv"

    //现在dataFrame 读入展示
     val df = sqlContext
    .read
    .format(formatType)
      .option("header","false")
    .load(cvsPath)

    df.show(1)

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



  }

}
case class Taxi(id: Int, longitude: Float,latitude :Float,count:Int)
